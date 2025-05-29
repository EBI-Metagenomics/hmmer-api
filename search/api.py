import logging
import io

from celery import chain, group
from django_celery_results.models import TaskResult
from django.http import HttpRequest, HttpResponse
from django.db import transaction
from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, Schema, Field
from pydantic import UUID4, ValidationInfo, field_validator
from pydantic_core import PydanticCustomError
from pyhmmer.easel import SequenceFile, MSAFile, TextSequence
from pyhmmer.plan7 import HMMFile
from typing import List

from architecture.tasks import build_architecture, build_annotation
from taxonomy.tasks import build_taxonomy_tree, build_taxonomy_distribution_graph
from .tasks import run_search
from .models import HmmerJob, Database
from .schema import ValidationErrorSchema


logger = logging.getLogger(__name__)

router = Router()


class TaskResultSchema(ModelSchema):
    class Meta:
        model = TaskResult
        fields = ["status", "date_created", "date_done"]


class DatabaseResponseSchema(ModelSchema):
    class Meta:
        model = Database
        fields = "__all__"


@router.get("/databases", response=List[DatabaseResponseSchema], tags=["search"])
def get_databases(request):
    return Database.objects.all().order_by("order")


class JobDetailsResponseSchema(ModelSchema):
    task: TaskResultSchema
    database: DatabaseResponseSchema

    class Meta:
        model = HmmerJob
        exclude = [
            "taxonomy_distribution_task",
            "taxonomy_tree_task",
            "taxonomy_distribution_graph_task",
            "architecture_task",
            "annotation_task",
            "with_taxonomy",
            "with_architecture",
        ]


@router.get("/{uuid:id}", response=JobDetailsResponseSchema, tags=["search"])
def get_job_details(request, id: str):
    job = get_object_or_404(HmmerJob, id=id)

    return job


@router.get("/{uuid:id}/query", tags=["search"])
def get_job_query(request, id: str):
    job = get_object_or_404(HmmerJob, id=id)

    response = HttpResponse(job.input, content_type="text/plain")

    response["Content-Disposition"] = 'inline; filename="query.txt"'

    return response


class SearchRequestSchema(ModelSchema):
    database_id: str = Field(alias="database")

    @field_validator("input", mode="after", check_fields=False)
    @classmethod
    def check_input(cls, value: str, info: ValidationInfo):
        algo = info.context["request"].get_full_path_info().split("/")[-1]

        if algo == HmmerJob.AlgoChoices.PHMMER or algo == HmmerJob.AlgoChoices.HMMSCAN:
            value_with_header = value if value.startswith(">") else f">Unnamed query\n{value}"

            try:
                with SequenceFile(io.BytesIO(value_with_header.encode()), format="fasta") as fh:
                    fh.guess_alphabet()
            except ValueError:
                raise PydanticCustomError("invalid_input", "Sequence is not valid")

            with SequenceFile(io.BytesIO(value_with_header.encode()), format="fasta") as fh:
                block = fh.read_block()

                if len(block) > 1:
                    raise PydanticCustomError("invalid_input", "Only one sequence is allowed")

                input_sequence: TextSequence = block[0]

                if not input_sequence.sequence.strip():
                    raise PydanticCustomError("invalid_input", "Sequence is not valid")

            return value_with_header

        if algo == HmmerJob.AlgoChoices.HMMSEARCH:
            try:
                with HMMFile(io.BytesIO(value.encode())) as fh:
                    fh.is_pressed()
                is_valid_hmm = True
            except ValueError:
                is_valid_hmm = False

            if is_valid_hmm:
                return value

            try:
                with SequenceFile(io.BytesIO(value.encode()), format="fasta") as fh:
                    block = fh.read_block()
                    is_fasta = True
            except ValueError:
                is_fasta = False

            if is_fasta:
                raise PydanticCustomError("invalid_input", "Invalid input")

            try:
                with MSAFile(io.BytesIO(value.encode())) as fh:
                    fh.guess_alphabet()
                is_valid_msa = True
            except ValueError:
                is_valid_msa = False

            if is_valid_msa:
                return value

            if not is_valid_msa:
                raise PydanticCustomError("invalid_input", "Alignment is not valid")

            if not is_valid_hmm:
                raise PydanticCustomError("invalid_input", "HMM is not valid")

    class Meta:
        model = HmmerJob
        exclude = [
            "id",
            "database",
            "task",
            "taxonomy_distribution_task",
            "taxonomy_tree_task",
            "taxonomy_distribution_graph_task",
            "architecture_task",
            "annotation_task",
            "algo",
        ]


class SearchResponseSchema(Schema):
    id: UUID4


@router.post("{algo}", response={200: SearchResponseSchema, 422: ValidationErrorSchema}, tags=["search"])
def search(request: HttpRequest, algo: HmmerJob.AlgoChoices, body: SearchRequestSchema):
    job = HmmerJob(**body.dict(), algo=algo)

    job.clean()
    job.save()

    request.session["job_ids"] = request.session.get("job_ids", []) + [str(job.id)]

    subsequent_tasks = []

    if job.algo != HmmerJob.AlgoChoices.HMMSCAN and job.with_taxonomy:
        subsequent_tasks += [
            build_taxonomy_tree.si(job.id),
            build_taxonomy_distribution_graph.si(job.id),
        ]

    if job.algo != HmmerJob.AlgoChoices.HMMSCAN and job.with_architecture:
        subsequent_tasks += [build_architecture.si(job.id)]

    if job.algo != HmmerJob.AlgoChoices.HMMSEARCH:
        subsequent_tasks += [build_annotation.si(job.id)]

    workflow = chain(
        run_search.si(job.id),
        group(*subsequent_tasks),
    )

    transaction.on_commit(lambda: workflow.delay())

    return {"id": job.id}


class JobsResponseSchema(ModelSchema):
    task: TaskResultSchema

    class Meta:
        model = HmmerJob
        fields = ["id", "algo"]


@router.get("", response=List[JobsResponseSchema], tags=["search"])
def get_jobs(request):
    job_ids = request.session.get("job_ids", [])

    return HmmerJob.objects.filter(id__in=job_ids).select_related("task").order_by("-task__date_created")
