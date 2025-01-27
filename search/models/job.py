import uuid
import logging
from django.db import models
from django.core.files.storage import storages
from django.dispatch import receiver
from django_celery_results.models import TaskResult

logger = logging.getLogger(__name__)


class HmmerJob(models.Model):
    """Base model for all jobs related to hmmer"""

    ALGO_CHOICES = [
        ("PHMMER", "Phmmer"),
        ("HMMSEARCH", "Hmmsearch"),
        ("HMMSCAN", "Hmmscan"),
        ("JACKHMMER", "Jackhmmer"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, unique=True)

    task = models.OneToOneField(
        TaskResult,
        to_field="task_id",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="hmmer_job",
    )

    algo = models.CharField(max_length=20, choices=ALGO_CHOICES)
    params = models.JSONField(default=dict)

    result_pkl = models.FileField(
        upload_to="%Y/%m/%d",
        storage=storages["results"],
    )
    result_json = models.FileField(
        null=True,
        blank=True,
        upload_to="%Y/%m/%d",
        storage=storages["results"],
    )

    def get_hmmpgmd_kwargs(self):
        return {}

    def get_hmmpgmd_connection_params(self):
        raise NotImplementedError()


# @receiver(models.signals.post_save, sender=TaskResult)
# def handle_task_saved(sender, instance, created, **kwargs):
#     if created:
#         logger.info(f"TaskResult {instance.task_id} created")
#         job = HmmerJob.objects.get(id=instance.task_id)
#         job.task = instance
#         job.save()
