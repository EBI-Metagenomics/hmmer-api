from ninja import Router
from django.urls import reverse
from django.db import transaction
from celery import chain

from search.schema import HmmerJobCreatedSchema
from .models import PhmmerJob
from .schema import PhmmerJobSchema

from phmmer.tasks import run_phmmer
from taxonomy.tasks import build_taxonomy_tree

router = Router()


@router.post("", response={202: HmmerJobCreatedSchema}, tags=["search"])
def search(request, data: PhmmerJobSchema):
    job = PhmmerJob.objects.create(params=data.dict())
    transaction.on_commit(
        lambda: chain(
            run_phmmer.si(job.id).set(task_id=job.id),
            build_taxonomy_tree.si(job.id),
        ).delay()
    )

    return HmmerJobCreatedSchema(
        id=job.id,
        status="PENDING",
        status_url=request.build_absolute_uri(reverse("api-1.0.0:status", kwargs={"id": job.id})),
    )
