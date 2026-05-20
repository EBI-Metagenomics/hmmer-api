import json
import shutil
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from conftest import apply_task
from download.models import FileJob
from download.tasks import generate
from search.tests.factories import HmmerJobFactory


@pytest.fixture
def completed_job(db, pdb_database, hits_bin_path, tmp_storage, hmmer_pdb_settings):
    from django.core.files.storage import storages

    job = HmmerJobFactory(database=pdb_database)
    task = TaskResult.objects.create(
        task_id=str(uuid.uuid4()), status="SUCCESS", result='"done"'
    )
    storage = storages["results"]
    path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))
    shutil.copy(hits_bin_path, storage.path(path))

    job.task = task
    job.result_path = storage.path(path)
    job.save(update_fields=["task", "result_path"])
    return job


@pytest.mark.django_db(transaction=True)
class TestGenerateTask:
    def test_generate_tsv_creates_file(self, completed_job, tmp_storage):
        file_job = FileJob.objects.create(
            job=completed_job,
            format="tsv",
            filters={"taxonomy_ids": None, "architecture": None},
        )
        apply_task(generate, args=[str(file_job.id)])
        file_job.refresh_from_db()
        assert file_job.task is not None
        assert file_job.task.status == "SUCCESS"

    def test_generate_text_creates_file(self, completed_job, tmp_storage):
        file_job = FileJob.objects.create(
            job=completed_job,
            format="text",
            filters={"taxonomy_ids": None, "architecture": None},
        )
        apply_task(generate, args=[str(file_job.id)])
        file_job.refresh_from_db()
        assert file_job.task.status == "SUCCESS"

    def test_generate_fasta_uses_fasta_strategy(self, completed_job, tmp_storage):
        file_job = FileJob.objects.create(
            job=completed_job,
            format="fasta",
            filters={"taxonomy_ids": None, "architecture": None},
        )
        with patch("download.tasks.FastaBuildStrategy") as mock_strategy_class:
            mock_strategy = mock_strategy_class.return_value
            apply_task(generate, args=[str(file_job.id)])
            mock_strategy.build.assert_called_once()
