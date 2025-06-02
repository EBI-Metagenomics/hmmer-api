import logging
from socket import gaierror

from django.conf import settings
from django_celery_results.models import TaskResult
from django.core.files.base import ContentFile
from django.core.files.storage import storages

from hmmerapi.celery import app
from search.client import Client, HmmpgmdServerError
from .models import HmmerJob

logger = logging.getLogger(__name__)


@app.task(bind=True)
def run_search(self, job_id: str):
    logger.debug(f"Running job {job_id}")

    job = HmmerJob.objects.select_related("database").get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.task = task_result
    job.save(update_fields=["task"])

    try:
        db_config = settings.HMMER.databases[job.database.id]
    except KeyError:
        raise ValueError(f"Database {job.database.id} not found in settings")

    storage = storages["results"]

    path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))

    try:
        with Client(address=db_config.host, port=db_config.port) as client:
            client.search(
                db_cmd=job.hmmpgmd_db,
                parameters=job.hmmpgmd_parameters,
                query=job.hmmpgmd_query,
                path=storage.path(path),
            )
    except (HmmpgmdServerError, ConnectionError, gaierror) as e:
        logger.warning(e)
        storage.delete(f"{job.id}/hits.bin")

        raise self.retry(exc=e, max_retries=5, countdown=30 * 60)

    return storage.path(path)
