import pickle
import logging
from pyhmmer.daemon import Client
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from hmmerapi.celery import app

from .models import PhmmerJob

logger = logging.getLogger(__name__)


@app.task(bind=True)
def run_phmmer(self, search_id: str):
    logger.info(f"Running phmmer job {search_id}")
    job = PhmmerJob.objects.get(id=search_id)
    task_result = TaskResult.objects.get(task_id=search_id)
    job.task = task_result
    job.save()

    with Client(**job.get_hmmpgmd_connection_params()) as client:
        top_hits = client.search_seq(**job.get_hmmpgmd_kwargs())

    job.result_pkl.save(f"{search_id}.pkl", ContentFile(pickle.dumps(top_hits)))
