import logging
from django.urls import reverse
from django.shortcuts import get_object_or_404
from ninja import Router
from celery.states import SUCCESS, EXCEPTION_STATES
from .models import HmmerJob
from .schema import HmmerJobStatusSchema

logger = logging.getLogger(__name__)

router = Router()


@router.get("/{uuid:id}", response=HmmerJobStatusSchema, tags=["search"])
def status(request, id: str):
    search_job = get_object_or_404(HmmerJob, id=id)

    return HmmerJobStatusSchema(
        id=search_job.id,
        status=search_job.task.status,
        result_url=(
            request.build_absolute_uri(reverse("api-1.0.0:get_result", kwargs={"id": search_job.id}))
            if search_job.task.status == SUCCESS
            else None
        ),
        error_message=search_job.task.traceback if search_job.task.status in EXCEPTION_STATES else None,
    )
