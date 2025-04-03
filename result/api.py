import json
import logging
import math

from celery.states import SUCCESS, PENDING
from django.conf import settings
from django.shortcuts import get_object_or_404
from ninja import Router, Schema, Query, Field
from typing import List, Optional

from search.models import HmmerJob
from .models import Result, P7Domain, post_process_pfam

logger = logging.getLogger(__name__)

router = Router()


class ResultQuerySchema(Schema):
    page: int = Field(default=1, gt=0)
    page_size: int = Field(default=50, gt=0)
    taxonomy_ids: Optional[List[int]] = Field(default=None)
    architecture: Optional[str] = Field(default=None)
    with_domains: Optional[bool] = Field(default=False)


class ResultResponseSchema(Schema):
    status: str
    result: Optional[Result] = Field(default=None)
    page_count: Optional[int] = Field(default=None)


class AlignmentQuerySchema(Schema):
    index: int = Field(default=0, ge=0)


class AlignmentResponseSchema(Schema):
    status: str
    domains: Optional[List[P7Domain]]


@router.get("/{uuid:id}", response=ResultResponseSchema, tags=["result"])
def get_result(request, id: str, query: Query[ResultQuerySchema]):
    job = get_object_or_404(HmmerJob, id=id)

    try:
        status = job.task.status
    except AttributeError:
        status = PENDING

    if status != SUCCESS:
        return {"status": status}

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, total_count = Result.from_file(
        json.loads(job.task.result),
        start=(query.page - 1) * query.page_size,
        end=query.page * query.page_size,
        db_conf=db_config,
        taxonomy_ids=query.taxonomy_ids,
        with_domains=query.with_domains or job.algo == HmmerJob.AlgoChoices.HMMSCAN,
        architecture=query.architecture,
        algo=job.algo,
    )

    if job.algo == HmmerJob.AlgoChoices.HMMSCAN:
        post_process_pfam(result)

    return {"status": status, "result": result, "page_count": math.ceil(total_count / query.page_size)}


@router.get("/{uuid:id}/domains", response=AlignmentResponseSchema, tags=["result"])
def get_domains(request, id: str, query: Query[AlignmentQuerySchema]):
    job = HmmerJob.objects.get(id=id)

    try:
        status = job.task.status
    except AttributeError:
        status = PENDING

    if status != SUCCESS:
        return {"status": status}

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(
        json.loads(job.task.result),
        with_metadata=False,
        with_domains=True,
        start=query.index,
        end=query.index + 1,
        db_conf=db_config,
    )

    return {"status": status, "domains": result.hits[0].domains}
