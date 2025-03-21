import logging
from celery.states import UNREADY_STATES, EXCEPTION_STATES
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from ninja import Router, Schema
from typing import List, Optional

from search.models import HmmerJob
from .models import FileJob, allowed_formats, formats
from .tasks import generate

logger = logging.getLogger(__name__)

router = Router()


class DownloadsResponseSchema(Schema):
    format: str
    name: str
    description: str
    status: str
    url: Optional[str]
    size: Optional[int]


@router.post("/{uuid:id}/{format}", response={204: None}, tags=["download"])
def generate_file(request, id: str, format: str):
    hmmer_job = HmmerJob.objects.get(id=id)

    if hmmer_job.task.status in UNREADY_STATES:
        return 400, f"Job {id} is not finished"

    if hmmer_job.task.status in EXCEPTION_STATES:
        return 500, f"Job {id} has failed. Downloads are unavailable"

    if format not in allowed_formats[hmmer_job.algo]:
        return 400, f"Format {format} is not available for {hmmer_job.algo}"

    file_job, created = FileJob.objects.get_or_create(job=hmmer_job, format=format)

    if created:
        generate.delay_on_commit(file_job.id)

    return 204, None


@router.get("/{uuid:id}/{format}", tags=["download"])
def download_file(request, id: str, format):
    file_job = get_object_or_404(FileJob, job__id=id, format=format)

    status = "AVAILABLE"

    if file_job is None:
        status = "NOT_GENERATED"
    else:
        if file_job.task is None or file_job.task.status in UNREADY_STATES:
            status = "GENERATING"
        if file_job.task.status in EXCEPTION_STATES:
            status = "FAILED"

    if status != "AVAILABLE":
        return 400, "File now available"

    response = HttpResponse(file_job.file, "text/plain")
    response["Content-Length"] = file_job.file.size
    response["Content-Disposition"] = f'attachment; filename="{file_job.file.name}"'

    return response


@router.get("/{uuid:id}", response=List[DownloadsResponseSchema], tags=["download"])
def get_downloads(request, id: str):
    hmmer_job = HmmerJob.objects.get(id=id)

    if hmmer_job.task.status in UNREADY_STATES:
        return 400, f"Job {id} is not finished"

    if hmmer_job.task.status in EXCEPTION_STATES:
        return 500, f"Job {id} has failed. Downloads are unavailable"

    file_jobs = FileJob.objects.filter(job=hmmer_job)

    downloads = []

    for allowed_format in allowed_formats[hmmer_job.algo]:
        file_job = next((job for job in file_jobs if job.format == allowed_format), None)

        status = "AVAILABLE"

        if file_job is None:
            status = "NOT_GENERATED"
        else:
            if file_job.task is None or file_job.task.status in UNREADY_STATES:
                status = "GENERATING"
            if file_job.task.status in EXCEPTION_STATES:
                status = "FAILED"

        url = None
        size = None

        if status == "AVAILABLE":
            scheme = "https" if settings.BUILD_HTTPS_DOWNLOAD_URLS else request.scheme
            url = request.build_absolute_uri(f"{scheme}://{request.get_host()}{file_job.file.url}")
            size = file_job.file.size

        downloads.append(
            {
                "format": allowed_format,
                "name": formats[allowed_format]["name"],
                "description": formats[allowed_format]["description"],
                "status": status,
                "url": url,
                "size": size,
            }
        )

    return downloads
