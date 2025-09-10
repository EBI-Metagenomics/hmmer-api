import logging
import io
import uuid

from django_celery_results.models import TaskResult
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.db import transaction
from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, Schema, Field
from ninja.errors import HttpError
from pydantic import UUID4, EmailStr, ValidationInfo, model_validator, field_validator
from pydantic_core import PydanticCustomError
from pyhmmer.easel import SequenceFile, MSAFile
from pyhmmer.plan7 import HMMFile
from typing import List, Optional

from .models import HmmerJob, Database
from .schema import ValidationErrorSchema
from .tasks import schedule_next_iteration

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
    # TODO: filter out enabled/paused databases only. Do a check on job submission as well
    return Database.objects.all().order_by("order")


class JobDetailsResponseSchema(ModelSchema):
    task: Optional[TaskResultSchema]
    database: DatabaseResponseSchema
    iteration: Optional[int]
    next_job_id: Optional[UUID4]
    previous_job_id: Optional[UUID4]
    parent_job_id: Optional[UUID4]
    include: List[int]
    exclude: List[int]

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
            "parent",
            "include",
            "exclude",
            "result_path",
            "hits_index_path",
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
    input: str | UUID4
    input_type: Optional[str] = Field(default=None)
    database_id: Optional[str] = Field(default=None, alias="database")
    taxonomy_ids: Optional[List[int]] = Field(default=[])
    include: Optional[List[int]] = Field(default=[])
    exclude: Optional[List[int]] = Field(default=[])

    @field_validator("input", mode="before")
    @classmethod
    def validate_input(cls, v: str, info: ValidationInfo):
        algo = info.context["request"].get_full_path_info().split("/")[-1]

        if not hasattr(info.context, "validation_results"):
            info.context["validation_results"] = {}

        validated_input = None
        input_type = None

        if algo == HmmerJob.AlgoChoices.PHMMER or algo == HmmerJob.AlgoChoices.HMMSCAN:
            validated_input, input_type = cls.validate_sequence(v)
            info.context["validation_results"]["input_type"] = input_type

            return validated_input

        if algo == HmmerJob.AlgoChoices.HMMSEARCH:
            validators = [
                cls.validate_hmm,
                cls.validate_msa,
            ]

            for validator in validators:
                try:
                    validated_input, input_type = validator(v)

                    break
                except PydanticCustomError:
                    continue

            if input_type is None:
                raise PydanticCustomError("invalid_input", "Invalid HMM/MSA")
            else:
                info.context["validation_results"]["input_type"] = input_type
                return validated_input

        if algo == HmmerJob.AlgoChoices.JACKHMMER:
            validators = [
                cls.validate_uuid,
                cls.validate_sequence,
                cls.validate_hmm,
                cls.validate_msa,
            ]

            for validator in validators:
                try:
                    validated_input, input_type = validator(v)

                    break
                except PydanticCustomError:
                    continue

            if input_type in {
                HmmerJob.InputChoices.MULTI_SEQUENCE,
                HmmerJob.InputChoices.MULTI_HMM,
                HmmerJob.InputChoices.MULTI_MSA,
            }:
                raise PydanticCustomError("invalid_input", "Multiple queries are not allowed for jackhmmer")
            elif input_type is None:
                raise PydanticCustomError("invalid_input", "Invalid jackhmmer input")
            else:
                info.context["validation_results"]["input_type"] = input_type
                return validated_input

    @model_validator(mode="after")
    def set_input_type_from_context(self, info: ValidationInfo):
        if "validation_results" in info.context:
            if "input_type" in info.context["validation_results"]:
                self.input_type = info.context["validation_results"]["input_type"]

        return self

    @field_validator("iterations", mode="before", check_fields=False)
    @classmethod
    def validate_iterations(cls, value: int | None, info: ValidationInfo):
        if value is not None and (value < 0 or value > settings.HMMER.jackhmmer_max_batch_iterations):
            raise HttpError(400, "Number of iterations for jackhmmer is not valid")

        return value

    @classmethod
    def validate_sequence(cls, input: str):
        input_with_header = input if input.startswith(">") else f">Unnamed query\n{input}"

        try:
            with SequenceFile(io.BytesIO(input_with_header.encode()), format="fasta") as fh:
                fh.guess_alphabet()
        except ValueError:
            raise PydanticCustomError("invalid_input", "Sequence input is not valid")

        with SequenceFile(io.BytesIO(input_with_header.encode()), format="fasta") as fh:
            block = fh.read_block()

            if len(block) > settings.HMMER.max_queries:
                raise PydanticCustomError(
                    "invalid_input", f"Input contains more than {settings.HMMER.max_queries} sequences"
                )

            for i, sequence in enumerate(block):
                if not sequence.sequence.strip():
                    raise PydanticCustomError("invalid_input", f"Sequence {i + 1} is not valid")

                if len(sequence.sequence.strip()) > settings.HMMER.max_sequence_base_pairs:
                    raise PydanticCustomError(
                        "invalid_input",
                        f"Sequence {i + 1} is longer than {settings.HMMER.max_sequence_base_pairs} base pairs",
                    )

                if sequence.accession is None:
                    raise PydanticCustomError("invalid_input", f"Sequence {i + 1} has no valid accession")

            if len(block) > 1:
                return input_with_header, HmmerJob.InputChoices.MULTI_SEQUENCE
            else:
                return input_with_header, HmmerJob.InputChoices.SEQUENCE

    @classmethod
    def validate_hmm(cls, input: str):
        try:
            with HMMFile(io.BytesIO(input.encode())) as fh:
                fh.is_pressed()
        except ValueError:
            raise PydanticCustomError("invalid_input", "HMM input is not valid")

        with HMMFile(io.BytesIO(input.encode())) as fh:
            hmms = []
            i = 0

            while (hmm := fh.read()) is not None:
                hmms.append(hmm)

                try:
                    hmm.validate()
                except ValueError:
                    raise PydanticCustomError("invalid_input", f"HMM {i + 1} is not valid")

                if hmm.consensus is None:
                    raise PydanticCustomError("invalid_input", f"HMM {i + 1} does not have a consensus")

                i += 1

            if len(hmms) > settings.HMMER.max_queries:
                raise PydanticCustomError(
                    "invalid_input", f"Input contains more than {settings.HMMER.max_queries} HMMs"
                )

            if len(hmms) > 1:
                return input, HmmerJob.InputChoices.MULTI_HMM
            else:
                return input, HmmerJob.InputChoices.HMM

    @classmethod
    def validate_msa(cls, input: str):
        try:
            with MSAFile(io.BytesIO(input.encode())) as fh:
                fh.guess_alphabet()
        except ValueError:
            raise PydanticCustomError("invalid_input", "MSA is not valid")

        with MSAFile(io.BytesIO(input.encode())) as fh:
            msas = []
            i = 0

            while (msa := fh.read()) is not None:
                msas.append(msa)

                if len(msa.alignment) == 0:
                    raise PydanticCustomError("invalid_input", f"MSA {i + 1} is not valid")

                i += 1

            if len(msas) > settings.HMMER.max_queries:
                raise PydanticCustomError(
                    "invalid_input", f"Input contains more than {settings.HMMER.max_queries} MSAs"
                )

            if len(msas) > 1:
                return input, HmmerJob.InputChoices.MULTI_MSA
            else:
                return input, HmmerJob.InputChoices.MSA

    @classmethod
    def validate_uuid(cls, input: str):
        try:
            if uuid.UUID(input).version == 4:
                return input, HmmerJob.InputChoices.UUID
        except (ValueError, AttributeError):
            raise PydanticCustomError("invalid_input", "UUID is not valid")

    @model_validator(mode="after")
    def validate_database_id_requirement(self):
        if self.input_type != HmmerJob.InputChoices.UUID and self.database_id is None:
            raise PydanticCustomError("missing_database_id", "database is required when input is not a UUID4")

        return self

    class Meta:
        model = HmmerJob
        fields = [
            "threshold",
            "E",
            "domE",
            "T",
            "domT",
            "incE",
            "incdomE",
            "incT",
            "incdomT",
            "popen",
            "pextend",
            "mx",
            "with_taxonomy",
            "with_architecture",
            "iterations",
            "exclude_all",
            "email_address",
        ]
        fields_optional = "__all__"


class SearchResponseSchema(Schema):
    id: UUID4


@router.post("/{algo}", response={200: SearchResponseSchema, 422: ValidationErrorSchema}, tags=["search"])
def search(request: HttpRequest, algo: HmmerJob.AlgoChoices, body: SearchRequestSchema):
    if algo == HmmerJob.AlgoChoices.JACKHMMER and body.input_type == HmmerJob.InputChoices.UUID:
        job = get_object_or_404(HmmerJob.objects.select_related("database", "parent"), id=body.input)

        job.include = body.include
        job.exclude = body.exclude
        job.exclude_all = body.exclude_all
        job.iterations = None
        job.save(update_fields=["include", "exclude", "exclude_all", "iterations"])

        schedule_next_iteration.apply(args=(job.id,))

        new_job = job.get_first_child()

        if new_job is None:
            raise HttpError(400, "Given job already converged.")

        return {"id": new_job.id}
    else:
        job = HmmerJob(**body.dict(exclude_unset=True), algo=algo)

        job.full_clean()
        job.save()

        request.session["job_ids"] = request.session.get("job_ids", []) + [str(job.id)]

    workflow = job.get_workflow()

    transaction.on_commit(lambda: workflow.delay())

    return {"id": job.id}


class SearchPatchSchema(Schema):
    email_address: Optional[EmailStr] = None


@router.patch("/{uuid:id}", response={204: None, 422: ValidationErrorSchema}, tags=["search"])
def update_search(request: HttpRequest, id: str, body: SearchPatchSchema):
    job = HmmerJob.objects.get(id=id)
    updated_fields = body.dict(exclude_unset=True)

    for attr, value in updated_fields.items():
        setattr(job, attr, value)

    job.save()

    return 204, None


class JobsResponseSchema(ModelSchema):
    task: Optional[TaskResultSchema]
    query_name: str

    class Meta:
        model = HmmerJob
        fields = ["id", "algo", "date_submitted"]


@router.get("", response=List[JobsResponseSchema], tags=["search"])
def get_jobs(request):
    job_ids = request.session.get("job_ids", [])

    return HmmerJob.objects.filter(id__in=job_ids).select_related("task").order_by("-date_submitted")
