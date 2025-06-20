import logging
import json
import hashlib

from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult
from hmmerapi.celery import app

from .models import formats, FileJob, FastaBuildStrategy, TemplateBuildStrategy, MSABuildStrategy

logger = logging.getLogger(__name__)


@app.task(bind=True)
def generate(self, job_id: str):
    logger.debug(f"Generating file {job_id}")
    file_job = FileJob.objects.get(id=job_id)

    task_result = TaskResult.objects.get(task_id=self.request.id)
    file_job.task = task_result
    file_job.save()

    filters_hash = hashlib.sha1(json.dumps(file_job.filters, sort_keys=True).encode()).hexdigest()[:16]

    file_name = f"{file_job.job.id}-{filters_hash}.{formats[file_job.format]["extension"]}"

    if "gzip" in formats[file_job.format] and formats[file_job.format]["gzip"]:
        file_name += ".gz"

    file_job.file.save(
        file_name, ContentFile(b"", "")
    )

    if file_job.format in {"tsv", "text"}:
        build_strategy = TemplateBuildStrategy(file_job)

    if file_job.format in {"fasta", "fullfasta"}:
        build_strategy = FastaBuildStrategy(file_job)

    if file_job.format in {"afa", "stockholm", "clustal", "psiblast", "phylip"}:
        build_strategy = MSABuildStrategy(file_job)

    build_strategy.build(file_job.file.path)
