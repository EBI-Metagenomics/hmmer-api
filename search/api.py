import logging
import io

from celery import chain
from django_celery_results.models import TaskResult
from django.http import HttpRequest
from django.db import transaction
from ninja import Router, ModelSchema, Schema
from pydantic import UUID4, ValidationInfo, field_validator
from pydantic_core import PydanticCustomError
from pyhmmer.easel import SequenceFile, MSAFile
from pyhmmer.plan7 import HMMFile
from typing import List

from architecture.tasks import build_architecture, build_annotation
from taxonomy.tasks import build_taxonomy_distribution, build_taxonomy_tree, build_taxonomy_distribution_graph
from .tasks import run_search
from .models import HmmerJob

logger = logging.getLogger(__name__)

router = Router()


class SearchRequestSchema(ModelSchema):
    @field_validator("input", mode="after", check_fields=False)
    @classmethod
    def check_input(cls, value: str, info: ValidationInfo):
        algo = info.context["request"].get_full_path_info().split("/")[-1]

        if algo == HmmerJob.AlgoChoices.PHMMER or algo == HmmerJob.AlgoChoices.HMMSCAN:
            try:
                with SequenceFile(io.BytesIO(value.encode())) as fh:
                    fh.guess_alphabet()

                return value
            except ValueError:
                raise PydanticCustomError("invalid_input", "Sequence is not valid")

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
        exclude = ["id", "task", "taxonomy_distribution_task", "taxonomy_tree_task", "algo"]


class SearchResponseSchema(Schema):
    id: UUID4


class TaskResultSchema(ModelSchema):
    class Meta:
        model = TaskResult
        fields = ["status", "date_created", "date_done"]


class JobsResponseSchema(ModelSchema):
    task: TaskResultSchema

    class Meta:
        model = HmmerJob
        fields = ["id", "algo"]


@router.post("{algo}", response=SearchResponseSchema, tags=["search"])
def search(request: HttpRequest, algo: HmmerJob.AlgoChoices, body: SearchRequestSchema):
    job = HmmerJob(algo=algo, **body.dict())

    job.clean()
    job.save()

    request.session["job_ids"] = request.session.get("job_ids", []) + [str(job.id)]

    tasks = [run_search.si(job.id)]

    if job.algo != HmmerJob.AlgoChoices.HMMSCAN and job.with_taxonomy:
        tasks += [
            build_taxonomy_distribution.si(job.id),
            build_taxonomy_tree.si(job.id),
            build_taxonomy_distribution_graph.si(job.id),
        ]

    if job.algo != HmmerJob.AlgoChoices.HMMSCAN and job.with_architecture:
        tasks += [build_architecture.si(job.id)]

    if job.algo != HmmerJob.AlgoChoices.HMMSEARCH:
        tasks += [build_annotation.si(job.id)]

    transaction.on_commit(lambda: chain(*tasks).delay())

    return {"id": job.id}


@router.get("", response=List[JobsResponseSchema], tags=["search"])
def get_jobs(request):
    job_ids = request.session.get("job_ids", [])

    return HmmerJob.objects.filter(id__in=job_ids).select_related("task").order_by("-task__date_created")
