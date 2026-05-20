import json
import shutil
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from conftest import apply_task
from search.models import Database, HmmerJob
from search.tasks import index_hits, notify_on_job_completion, run_search, schedule_next_iteration
from search.tests.factories import DatabaseFactory, HmmerJobFactory


@pytest.fixture
def pdb_job(db, pdb_database, tmp_storage, hmmer_pdb_settings):
    return HmmerJobFactory(database=pdb_database)


@pytest.mark.django_db(transaction=True)
class TestRunSearch:
    def test_saves_binary_result_to_disk(self, pdb_job, mock_client_with_hits, tmp_storage):
        apply_task(run_search, args=[str(pdb_job.id)])
        pdb_job.refresh_from_db()
        assert pdb_job.result_path is not None
        assert Path(pdb_job.result_path).exists()

    def test_links_task_result_to_job(self, pdb_job, mock_client_with_hits, tmp_storage):
        apply_task(run_search, args=[str(pdb_job.id)])
        pdb_job.refresh_from_db()
        assert pdb_job.task is not None
        assert pdb_job.task.status == "SUCCESS"

    def test_result_file_contains_hits_bin_content(self, pdb_job, mock_client_with_hits, tmp_storage, hits_bin_path):
        apply_task(run_search, args=[str(pdb_job.id)])
        pdb_job.refresh_from_db()
        assert Path(pdb_job.result_path).read_bytes() == hits_bin_path.read_bytes()


@pytest.mark.django_db(transaction=True)
class TestIndexHits:
    def test_creates_pkl_file(self, pdb_job, hits_bin_path, tmp_storage):
        from django.core.files.storage import storages
        storage = storages["results"]
        path = storage.save(f"{pdb_job.id}/hits.bin", ContentFile(b""))
        shutil.copy(hits_bin_path, storage.path(path))
        pdb_job.result_path = storage.path(path)
        pdb_job.save(update_fields=["result_path"])

        index_hits.apply(args=[str(pdb_job.id)])
        pdb_job.refresh_from_db()

        assert pdb_job.hits_index_path is not None
        assert Path(pdb_job.hits_index_path).exists()

    def test_pkl_contains_valid_index(self, pdb_job, hits_bin_path, tmp_storage):
        from django.core.files.storage import storages
        from result.models import HitsIndex

        storage = storages["results"]
        path = storage.save(f"{pdb_job.id}/hits.bin", ContentFile(b""))
        shutil.copy(hits_bin_path, storage.path(path))
        pdb_job.result_path = storage.path(path)
        pdb_job.save(update_fields=["result_path"])

        index_hits.apply(args=[str(pdb_job.id)])
        pdb_job.refresh_from_db()

        index = HitsIndex.from_file(pdb_job.hits_index_path)
        assert len(index.taxonomy_index) > 0


@pytest.mark.django_db(transaction=True)
class TestScheduleNextIteration:
    def test_creates_child_job(self, db, pdb_database, hmmer_pdb_settings, tmp_storage):
        job = HmmerJobFactory(
            database=pdb_database,
            algo=HmmerJob.AlgoChoices.JACKHMMER,
        )
        with patch("celery.canvas.Signature.delay"):
            schedule_next_iteration.apply(args=[str(job.id)])
        job.refresh_from_db()
        assert job.get_children_count() == 1

    def test_stops_at_max_iterations(self, db, settings, pdb_database, hmmer_pdb_settings):
        settings.HMMER.jackhmmer_max_iterations = 0
        job = HmmerJobFactory(
            database=pdb_database,
            algo=HmmerJob.AlgoChoices.JACKHMMER,
        )
        schedule_next_iteration.apply(args=[str(job.id)])
        job.refresh_from_db()
        assert job.get_children_count() == 0


@pytest.mark.django_db(transaction=True)
class TestNotifyOnJobCompletion:
    def test_skips_when_no_email(self, pdb_job, mailoutbox):
        pdb_job.email_address = None
        pdb_job.save(update_fields=["email_address"])

        notify_on_job_completion.apply(args=[str(pdb_job.id)])
        assert len(mailoutbox) == 0

    def test_sends_email_on_success(self, pdb_job, mailoutbox, settings):
        settings.EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"
        task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()),
            status="SUCCESS",
            result='"done"',
        )
        pdb_job.email_address = "user@example.com"
        pdb_job.task = task
        pdb_job.save(update_fields=["email_address", "task"])

        notify_on_job_completion.apply(args=[str(pdb_job.id)])

        assert len(mailoutbox) == 1
        assert mailoutbox[0].to == ["user@example.com"]
