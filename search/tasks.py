import logging
from socket import gaierror

import copy
from django.conf import settings
from django_celery_results.models import TaskResult
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django.db import transaction

from hmmerapi.celery import app
from search.client import Client, HmmpgmdServerError
from result.models import HmmdSearchStats
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

        stats = HmmdSearchStats.from_file(storage.path(path))
        job.number_of_hits = stats.nreported
        job.save(update_fields=["number_of_hits"])

    except (HmmpgmdServerError, ConnectionError, gaierror) as e:
        logger.warning(e)
        storage.delete(f"{job.id}/hits.bin")

        raise self.retry(exc=e, max_retries=5, countdown=30 * 60)

    return storage.path(path)


@app.task(bind=True)
def schedule_next_iteration(self, job_id: str):
    job = HmmerJob.objects.select_related("database", "parent").get(id=job_id)

    logger.debug(f"Creating iteration {job.iteration + 1} from job {job_id}")

    if job.iteration == settings.HMMER.jackhmmer_max_iterations:
        logger.debug(
            f"Jackhmmer job {job_id} is iteration {job.iteration}/{settings.HMMER.jackhmmer_max_iterations}. Stopping."
        )

        return

    if job.iteration > 0:
        convergence_stats = job.convergence_stats

        logger.debug(convergence_stats)

        if convergence_stats["gained"] == 0 and convergence_stats["dropped"] == 0 and convergence_stats["lost"] == 0:
            logger.debug(f"Jackhmmer job {job_id} converged. Stopping.")

            return

    existing_job = job.get_first_child()

    if existing_job:
        existing_job.delete()

    next_job = copy.copy(job)  # this is to create a copy
    next_job.input_type = HmmerJob.InputChoices.UUID
    next_job.input = job_id
    next_job.id = None
    next_job.pk = None
    next_job.parent = None
    next_job._state.adding = True
    next_job.task = None
    next_job.annotation_task = None
    next_job.architecture_task = None
    next_job.taxonomy_tree_task = None
    next_job.taxonomy_distribution_task = None
    next_job.taxonomy_distribution_graph_task = None
    next_job.include = []
    next_job.exclude = []

    next_job = job.add_child(instance=next_job)

    workflow = next_job.get_workflow()

    transaction.on_commit(lambda: workflow.delay())
