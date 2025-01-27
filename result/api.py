import logging
import pickle
from ninja import Router, Schema, Query, Field

# from typing import Any, List
from celery.states import UNREADY_STATES, EXCEPTION_STATES
from search.models import HmmerJob
from search.schema import MessageSchema
from result.models import Result

logger = logging.getLogger(__name__)

router = Router()


class QuerySchema(Schema):
    limit: int = Field(10, gt=0)
    offset: int = Field(0, ge=0)


client_error_codes = frozenset({404, 409, 410})


@router.get(
    "/{uuid:id}",
    response={200: Result, client_error_codes: MessageSchema},
    tags=["result"],
)
def get_result(request, id: str, query: Query[QuerySchema]):
    job = HmmerJob.objects.get(id=id)

    if job is None:
        return 404, {"message": f"Job {id} not found."}

    if job.task.status in UNREADY_STATES:
        return 409, {"message": f"Job {id} is still running. Please use the status endpoint to check the status."}

    if job.task.status in EXCEPTION_STATES:
        return 410, {"message": f"Job {id} failed."}

    with job.result_pkl.open("rb") as fh:
        top_hits = pickle.load(fh)

    return Result.from_top_hits(top_hits, job_params=job.params, index=slice(query.offset, query.offset + query.limit))
