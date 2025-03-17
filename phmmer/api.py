# import logging
# from ninja import Router, ModelSchema, Schema
# from pydantic import UUID4
# from django.db import transaction
# from celery import chain

# from phmmer.tasks import run_phmmer
# from taxonomy.tasks import build_taxonomy_tree
# from search.models.job import SearchParameters
# from .models import PhmmerJob

# logger = logging.getLogger(__name__)

# router = Router()


# class RequestSchema(ModelSchema):
#     class Meta:
#         model = PhmmerJob
#         exclude = ["id", "task"]


# class ResponseSchema(Schema):
#     id: UUID4


# @router.post("", response=ResponseSchema, tags=["search"])
# def search(request, body: RequestSchema):
#     job = PhmmerJob.objects.create(**body.dict())

#     transaction.on_commit(
#         lambda: chain(
#             run_phmmer.si(job.id).set(task_id=job.id),
#             build_taxonomy_tree.si(job.id),
#         ).delay()
#     )

#     return {"id": job.id}
