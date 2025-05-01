import json
import logging
import math
from hashlib import md5

from django.db.models.functions import MD5
from django.conf import settings
from celery.states import SUCCESS, PENDING
from ninja import Router, ModelSchema, Schema, Field, Query
from typing import List, Optional

from search.models import HmmerJob
from result.models import Result
from .models import Architecture, Annotation

logger = logging.getLogger(__name__)

router = Router()


class ArchitectureSchema(ModelSchema):
    sequence_accession: Optional[str] = None
    sequence_external_link: Optional[str] = None

    class Meta:
        model = Architecture
        fields = ["names", "score", "graphics", "accessions"]


class ArchitectureAggregationSchema(Schema):
    count: int
    architecture: ArchitectureSchema


class ArchitectureResponseSchema(Schema):
    status: str
    architectures: Optional[List[ArchitectureAggregationSchema]] = Field(default=None)
    page_count: Optional[int] = Field(default=None)


class ArchitectureListResponseSchema(Schema):
    status: str
    architectures: Optional[List[ArchitectureSchema]]


class ArchitectureAnnotationsResponseSchema(Schema):
    status: str
    annotations: Optional[List[Annotation]] = Field(default=None)


class ArchitectureQuerySchema(Schema):
    page: int = Field(default=1, gt=0)
    page_size: int = Field(default=50, gt=0)


@router.get("/name/{accessions}", response=ArchitectureSchema, tags=["architecture"])
def get_architecture_name(request, accessions: str):
    accessions_md5 = md5(accessions.encode()).hexdigest()

    architecture = (
        Architecture.objects.annotate(accessions_md5=MD5("accessions")).filter(accessions_md5=accessions_md5).first()
    )
    return architecture


@router.get("/{uuid:id}", response=ArchitectureResponseSchema, tags=["architecture"])
def get_domain_architectures(request, id: str, query: Query[ArchitectureQuerySchema]):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        architecture_status = job.architecture_task.status
    except AttributeError:
        architecture_status = PENDING

    if search_status != SUCCESS or architecture_status != SUCCESS:
        return {"status": architecture_status}

    with open(json.loads(job.architecture_task.result), "rt") as fh:
        architectures = json.load(fh)

        architectures_count = len(architectures)

        start = (query.page - 1) * query.page_size
        end = query.page * query.page_size

        if end > architectures_count:
            end = architectures_count

        return {
            "status": SUCCESS,
            "architectures": architectures[start:end],
            "page_count": math.ceil(architectures_count / query.page_size),
        }


@router.get("/{uuid:id}/annotations", response=ArchitectureAnnotationsResponseSchema, tags=["architecture"])
def get_annotations(request, id: str):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        annotations_status = job.annotation_task.status
    except AttributeError:
        annotations_status = PENDING

    if search_status != SUCCESS or annotations_status != SUCCESS:
        return {"status": annotations_status}

    with open(json.loads(job.annotation_task.result), "rt") as fh:
        return {"status": SUCCESS, "annotations": [json.load(fh)]}


@router.get("/{uuid:id}/{accessions}", response=ArchitectureListResponseSchema, tags=["architecture"])
def get_all_architectures(request, id: str, accessions: str):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        architecture_status = job.architecture_task.status
    except AttributeError:
        architecture_status = PENDING

    if search_status != SUCCESS or architecture_status != SUCCESS:
        return {"status": architecture_status}

    try:
        db_config = settings.HMMER.databases[job.database.id]
    except KeyError:
        raise ValueError(f"Database {job.database.id} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config, architecture=accessions)
    sequence_indexes = [int(hit.name) for hit in result.hits]

    architectures = Architecture.objects.filter(sequence_index__in=sequence_indexes, database=job.database.id).order_by(
        "-score"
    )

    return {"status": SUCCESS, "architectures": architectures}
