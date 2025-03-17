import json
import logging
import math

from celery.states import SUCCESS, PENDING
from django.conf import settings
from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, Schema, Query, Field
from typing import List, Optional

from search.models import HmmerJob
from .models import Architecture, Annotation

logger = logging.getLogger(__name__)

router = Router()


class ArchitectureSchema(ModelSchema):
    class Meta:
        model = Architecture
        fields = ["names", "score", "graphics", "accessions"]


class ArchitectureAggregationSchema(Schema):
    count: int
    architecture: ArchitectureSchema


class ArchitectureResponseSchema(Schema):
    status: str
    architectures: Optional[List[ArchitectureAggregationSchema]]


class ArchitectureListResponseSchema(Schema):
    status: str
    architectures: Optional[List[ArchitectureSchema]]


class ArchitectureAnnotationsResponseSchema(Schema):
    status: str
    annotations: Optional[List[Annotation]] = Field(default=None)


@router.get("/name/{accessions}", response=ArchitectureSchema, tags=["architecture"])
def get_architecture_name(request, accessions: str):
    architecture = Architecture.objects.filter(accessions=accessions).first()
    return architecture


@router.get("/{uuid:id}", response=ArchitectureResponseSchema, tags=["architecture"])
def get_domain_architectures(request, id: str):
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
        return {
            "status": SUCCESS,
            "architectures": sorted(
                [{"count": len(architectures), "architecture": architectures[0]} for architectures in json.load(fh)],
                key=lambda x: x["count"],
                reverse=True,
            ),
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
        return {
            "status": SUCCESS,
            "annotations": [json.load(fh)]
        }


@router.get("/{uuid:id}/{name}", response=ArchitectureListResponseSchema, tags=["architecture"])
def get_all_architectures(request, id: str, name: str):
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
        all_architectures = json.load(fh)

        [architectures] = [architectures for architectures in all_architectures if architectures[0]["names"] == name]

        return {"status": SUCCESS, "architectures": architectures}
