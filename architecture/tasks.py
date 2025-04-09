import json
import logging
from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django_celery_results.models import TaskResult
from hmmerapi.celery import app
from result.models import Result, post_process_pfam, predict_active_sites
from search.models import HmmerJob
from search.client import Client
from .models import Architecture, Annotation

logger = logging.getLogger(__name__)


@app.task(bind=True)
def build_architecture(self, job_id: str):
    logger.debug(f"Making architecture distribution for job {job_id}")

    job = HmmerJob.objects.get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.architecture_task = task_result
    job.save(update_fields=["architecture_task"])

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config)

    architectures = Architecture.from_results(result, db_config.architecture_database)

    storage = storages["results"]
    name = storage.save(f"{job.id}/architecture.json", ContentFile(""))

    with storage.open(name, mode="wt") as fh:
        json.dump(architectures, fh)

    return storage.path(name)


@app.task(bind=True)
def build_annotation(self, job_id: str):
    logger.debug(f"Making sequence features annotation for job {job_id}")

    job = HmmerJob.objects.get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.annotation_task = task_result
    job.save(update_fields=["annotation_task"])

    try:
        db_config = settings.HMMER.databases[settings.HMMER.annotation_db]
    except KeyError:
        raise ValueError(f"Database {settings.HMMER.annotation_db} not found in settings")

    with Client(address=db_config.host, port=db_config.port) as client:
        data = client.search(db_cmd="--hmmdb 1", parameters="--cut_ga", query=job.hmmpgmd_query)

    result, _ = Result.from_data(data, db_conf=db_config, with_domains=True, with_metadata=True)

    post_process_pfam(result)
    predict_active_sites(result)

    storage = storages["results"]
    name = storage.save(
        f"{job.id}/annotation.json", ContentFile(Annotation.from_results(result).model_dump_json())
    )
    return storage.path(name)
