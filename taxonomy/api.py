import logging
from django.core.cache import cache
from django.shortcuts import get_object_or_404
from ninja import Router

from search.models import HmmerJob
from .models import Taxonomy
from .tasks import build_taxonomy_tree
logger = logging.getLogger(__name__)

router = Router()


@router.get("", tags=["taxonomy"])
def get(request):
    tree = cache.get_or_set("taxonomy_tree", Taxonomy.dump, timeout=60 * 60 * 24)
    return tree


# @router.post("/{uuid:id}", tags=["taxonomy"])
# def make_taxonomy_tree(request, id: str):
#     hmmer_job = get_object_or_404(HmmerJob, id=id)
#     taxonomy = Taxonomy.objects.create(hmmer_job=hmmer_job)

#     run_phmmer.apply_async_on_commit(args=[job.id], task_id=job.id)

