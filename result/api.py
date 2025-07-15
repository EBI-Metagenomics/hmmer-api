import logging
import math
from celery.states import SUCCESS, PENDING
from django.conf import settings
from django.shortcuts import get_object_or_404
from ninja import Router, Schema, Query, Field
from typing import List, Optional, Dict, Annotated, Any
from pydantic import UUID4, Discriminator, Tag

from search.models import HmmerJob, Restrictions
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


class JackhmmerResponseSchema(Schema):
    id: UUID4
    status: str
    iteration: int
    convergence_stats: Optional[Dict[str, int]]


class BatchResponseSchema(Schema):
    id: UUID4
    query_name: str
    status: str


class AlignmentQuerySchema(Schema):
    index: int = Field(default=0, ge=0)


class AlignmentResponseSchema(Schema):
    status: str
    domains: Optional[List[P7Domain]]


def get_discriminator_value(v: Any):
    if isinstance(v, list):
        if len(v) > 0 and v[0].get("iteration", None) is None:
            return "batch"

        return "jackhmmer"

    return "other"


ResponseSchema = Annotated[
    (
        Annotated[ResultResponseSchema, Tag("other")]
        | Annotated[List[JackhmmerResponseSchema], Tag("jackhmmer")]
        | Annotated[List[BatchResponseSchema], Tag("batch")]
    ),
    Discriminator(get_discriminator_value),
]


@router.get("/{uuid:id}", response=ResponseSchema, tags=["result"])
def get_result(request, id: str, query: Query[ResultQuerySchema]):
    job = get_object_or_404(HmmerJob, id=id)

    if job.algo == HmmerJob.AlgoChoices.JACKHMMER and job.iteration == 0:
        descendants = job.get_descendants()

        return [
            {
                "id": descendant.id,
                "status": descendant.task.status if descendant.task is not None else PENDING,
                "iteration": descendant.iteration,
                "convergence_stats": descendant.convergence_stats,
            }
            for descendant in descendants
        ]

    if job.algo != HmmerJob.AlgoChoices.JACKHMMER and job.is_batch_mode:
        descendants = job.get_descendants()

        return [
            {
                "id": descendant.id,
                "query_name": descendant.query_name,
                "status": descendant.task.status if descendant.task is not None else PENDING,
            }
            for descendant in descendants
        ]

    try:
        status = job.task.status
    except AttributeError:
        status = PENDING

    if status != SUCCESS:
        return {"status": status}

    restrictions = Restrictions(
        start=(query.page - 1) * query.page_size,
        end=query.page * query.page_size,
        with_metadata=True,
        with_domains=query.with_domains or job.algo == HmmerJob.AlgoChoices.HMMSCAN,
        taxonomy_ids=query.taxonomy_ids,
        architecture=query.architecture,
    )

    job.set_restrictions(restrictions)
    result, total_count = job.get_result()

    if job.algo == HmmerJob.AlgoChoices.HMMSCAN:
        post_process_pfam(result)

    return {"status": status, "result": result, "page_count": math.ceil(total_count / query.page_size)}


@router.get("/{uuid:id}/domains", response=AlignmentResponseSchema, tags=["result"])
def get_domains(request, id: str, query: Query[AlignmentQuerySchema]):
    job = HmmerJob.objects.select_related("database").get(id=id)

    try:
        status = job.task.status
    except AttributeError:
        status = PENDING

    if status != SUCCESS:
        return {"status": status}

    try:
        db_config = settings.HMMER.databases[job.database.id]
    except KeyError:
        raise ValueError(f"Database {job.database.id} not found in settings")

    result, _ = Result.from_file(
        job.result_path,
        with_metadata=False,
        with_domains=True,
        start=query.index,
        end=query.index + 1,
        db_conf=db_config,
    )

    return {"status": status, "domains": result.hits[0].domains}
