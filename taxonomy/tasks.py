import json
import logging
from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django_celery_results.models import TaskResult
from pydantic import TypeAdapter
from hmmerapi.celery import app
from result.models import Result
from search.models import HmmerJob
from .models import TaxonomyTree, TaxonomyResult, TaxonomyDistributionGraph

logger = logging.getLogger(__name__)


@app.task(bind=True)
def build_taxonomy_tree(self, job_id: str):
    logger.debug(f"Making taxonomy tree for job {job_id}")

    job = HmmerJob.objects.get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.taxonomy_tree_task = task_result
    job.save()

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config)

    tree = TaxonomyTree.build_tree(result.hits)

    storage = storages["results"]
    name = storage.save(f"{job.id}/taxonomy_tree.json", ContentFile(""))

    with storage.open(name, mode="wt") as fh:
        fh.write(tree.model_dump_json())

    return storage.path(name)


@app.task(bind=True)
def build_taxonomy_distribution(self, job_id: str):
    logger.debug(f"Making taxonomy distribution for job {job_id}")

    job = HmmerJob.objects.get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.taxonomy_distribution_task = task_result
    job.save()

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config)

    distribution = TaxonomyResult.from_result(result)

    storage = storages["results"]
    name = storage.save(f"{job.id}/taxonomy_dist.json", ContentFile(""))

    with storage.open(name, mode="wb") as fh:
        adapter = TypeAdapter(TaxonomyResult)
        fh.write(adapter.dump_json(distribution))

    return storage.path(name)


@app.task(bind=True)
def build_taxonomy_distribution_graph(self, job_id: str):
    logger.debug(f"Making taxonomy distribution graph for job {job_id}")

    job = HmmerJob.objects.get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.taxonomy_distribution_graph_task = task_result
    job.save()

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config)

    distribution_graph = TaxonomyDistributionGraph.from_result(result)

    storage = storages["results"]
    name = storage.save(f"{job.id}/taxonomy_dist_graph.json", ContentFile(""))

    with storage.open(name, mode="wb") as fh:
        adapter = TypeAdapter(TaxonomyDistributionGraph)
        fh.write(adapter.dump_json(distribution_graph))

    return storage.path(name)
