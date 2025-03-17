# import logging
# from django.conf import settings
# from django_celery_results.models import TaskResult

# from hmmerapi.celery import app
# from search.client import Client
# from .models import PhmmerJob

# logger = logging.getLogger(__name__)


# @app.task(bind=True)
# def run_phmmer(self, job_id: str):
#     logger.debug(f"Running phmmer job {job_id}")

#     job = PhmmerJob.objects.get(id=job_id)
#     task_result = TaskResult.objects.get(task_id=self.request.id)
#     job.task = task_result
#     job.save()

#     [db_config] = [config for config in settings.HMMER.databases if config.name == job.database]

#     if db_config is None:
#         raise ValueError(f"No config found for {self.database}")

#     with Client(address=db_config.host, port=db_config.port) as client:
#         path = client.search(job)

#     return path
