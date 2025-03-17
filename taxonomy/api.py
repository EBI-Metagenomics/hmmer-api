import json
import logging
from typing import List, Optional
from django.conf import settings
from django.contrib.postgres.search import SearchQuery
from django.core.cache import cache
from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, Schema, Field
from celery.states import SUCCESS, PENDING
from search.models import HmmerJob
from result.models import Result
from .models import Taxonomy, TaxonomyResult, TaxonomyTree, TaxonomyDistributionGraph

logger = logging.getLogger(__name__)

router = Router()


class TaxonomyDistributionResponseSchema(Schema):
    status: str
    distribution: Optional[List[TaxonomyResult]] = Field(default=None)


class TaxonomyDistributionGraphResponseSchema(Schema):
    status: str
    graph: Optional[TaxonomyDistributionGraph] = Field(default=None)


class TaxonomyTreeResponseSchema(Schema):
    status: str
    tree: Optional[TaxonomyTree] = Field(default=None)


class TaxonomyResponseSchema(ModelSchema):
    class Meta:
        model = Taxonomy
        fields = ["taxonomy_id", "name", "rank"]


@router.get("", tags=["taxonomy"])
def get(request):
    tree = cache.get_or_set("taxonomy_tree", Taxonomy.dump, timeout=60 * 60 * 24)
    return tree


@router.get("/search", response=List[TaxonomyResponseSchema], tags=["taxonomy"])
def search_taxonomy(request, q: str):
    try:
        numerical_query = int(q)
    except ValueError:
        numerical_query = None

    queryset = Taxonomy.objects.filter(rank="species")

    if numerical_query is not None:
        return queryset.filter(taxonomy_id=numerical_query)

    words = q.split()
    search_query = SearchQuery(f"{words[0]}:*", search_type="raw")
    for word in words[1:]:
        search_query &= SearchQuery(f"{word}:*", search_type="raw")
    logger.debug(search_query)
    logger.debug(queryset.filter(search=search_query).query)
    return queryset.filter(search=search_query)[:10]


@router.get("/{id}", response=TaxonomyResponseSchema, tags=["taxonomy"])
def get_taxonomy(request, id: int, ):
    return get_object_or_404(Taxonomy, taxonomy_id=id)


@router.get("/{uuid:id}/distribution", response=TaxonomyDistributionResponseSchema, tags=["taxonomy"])
def get_taxonomy_distribution(request, id: str):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        taxonomy_distribution_status = job.taxonomy_tree_task.status
    except AttributeError:
        taxonomy_distribution_status = PENDING

    if search_status != SUCCESS or taxonomy_distribution_status != SUCCESS:
        return {"status": taxonomy_distribution_status}

    try:
        db_config = settings.HMMER.databases[job.database]
    except KeyError:
        raise ValueError(f"Database {job.database} not found in settings")

    result, _ = Result.from_file(json.loads(job.task.result), db_conf=db_config)

    return {"status": SUCCESS, "distribution": TaxonomyResult.from_result(result)}


@router.get("/{uuid:id}/tree", response=TaxonomyTreeResponseSchema, tags=["taxonomy"])
def get_taxonomy_tree(request, id: str):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        taxonomy_tree_status = job.taxonomy_tree_task.status
    except AttributeError:
        taxonomy_tree_status = PENDING

    if search_status != SUCCESS or taxonomy_tree_status != SUCCESS:
        return {"status": taxonomy_tree_status}

    with open(json.loads(job.taxonomy_tree_task.result), "rt") as fh:
        return {"status": SUCCESS, "tree": json.load(fh)}


@router.get("/{uuid:id}/disdtribution-graph", response=TaxonomyDistributionGraphResponseSchema, tags=["taxonomy"])
def get_taxonomy_distribution_graph(request, id: str):
    job = HmmerJob.objects.get(id=id)

    try:
        search_status = job.task.status
    except AttributeError:
        search_status = PENDING

    try:
        taxonomy_distribution_graph_status = job.taxonomy_distribution_graph_task.status
    except AttributeError:
        taxonomy_distribution_graph_status = PENDING

    if search_status != SUCCESS or taxonomy_distribution_graph_status != SUCCESS:
        return {"status": taxonomy_distribution_graph_status}

    with open(json.loads(job.taxonomy_distribution_graph_task.result), "rt") as fh:
        return {"status": SUCCESS, "graph": json.load(fh)}
