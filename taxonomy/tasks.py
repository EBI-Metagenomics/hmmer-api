import pickle
import logging
from pyhmmer.plan7 import TopHits
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from hmmerapi.celery import app
from result.models import Result
from search.models import HmmerJob
from .models import TaxonomyJob, Taxonomy

logger = logging.getLogger(__name__)


@app.task(bind=True)
def build_taxonomy_tree(self, hmmer_job_id: str):
    logger.info(f"Building taxonomy tree for hmmer job {hmmer_job_id}")
    hmmer_job = HmmerJob.objects.get(id=hmmer_job_id)

    if hmmer_job.task.status != "SUCCESS":
        raise Exception(f"Hmmer job {hmmer_job.task_id} has not finished yet")

    with hmmer_job.result_pkl.open("rb") as f:
        hits: TopHits = pickle.load(f)

    taxonomy_job = TaxonomyJob.objects.create(hmmer_job=hmmer_job)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    taxonomy_job.task = task_result
    taxonomy_job.save()

    seqdb = hmmer_job.params["seqdb"]

    result = Result.from_top_hits(hits, {"seqdb": seqdb})

    taxonomy_ids = set([hit.metadata.taxonomy_id for hit in result.hits])

    nodes = Taxonomy.objects.filter(taxonomy_id__in=taxonomy_ids)

    logger.info(nodes)
