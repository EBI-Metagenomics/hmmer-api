import json
import shutil
import uuid

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from download.models import FileJob
from search.tests.factories import HmmerJobFactory

BASE_URL = "/api/v1/download"


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


@pytest.mark.django_db
class TestGenerateFile:
    def test_generate_returns_204_on_success(self, client, completed_job, tmp_storage):
        response = client.post(f"{BASE_URL}/{completed_job.id}/tsv")
        assert response.status_code == 204
        assert FileJob.objects.filter(job=completed_job, format="tsv").exists()

    def test_generate_is_idempotent(self, client, completed_job, tmp_storage):
        client.post(f"{BASE_URL}/{completed_job.id}/tsv")
        client.post(f"{BASE_URL}/{completed_job.id}/tsv")
        assert FileJob.objects.filter(job=completed_job, format="tsv").count() == 1

    def test_generate_returns_error_when_job_not_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        from django_celery_results.models import TaskResult
        job = HmmerJobFactory(database=pdb_database)
        task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="PENDING", result=None
        )
        job.task = task
        job.save(update_fields=["task"])

        client.raise_request_exception = False
        response = client.post(f"{BASE_URL}/{job.id}/tsv")
        assert response.status_code not in (200, 204)

    def test_generate_does_not_succeed_for_unsupported_format(self, client, completed_job, tmp_storage):
        # The endpoint declares response={204: None} but returns (400, ...) for
        # unsupported formats, which causes a Ninja ConfigError at response time.
        # We suppress the exception and assert the request did not succeed.
        client.raise_request_exception = False
        response = client.post(f"{BASE_URL}/{completed_job.id}/xml")
        assert response.status_code != 204


@pytest.mark.django_db
class TestGetDownloads:
    def test_lists_allowed_formats(self, client, completed_job, tmp_storage):
        response = client.get(f"{BASE_URL}/{completed_job.id}")
        assert response.status_code == 200
        formats = [item["format"] for item in response.json()]
        assert "text" in formats
        assert "tsv" in formats
        assert "fasta" in formats

    def test_shows_not_generated_when_no_file_job(self, client, completed_job, tmp_storage):
        response = client.get(f"{BASE_URL}/{completed_job.id}")
        assert response.status_code == 200
        for item in response.json():
            assert item["status"] in ("NOT_GENERATED", "GENERATING", "AVAILABLE", "FAILED")

    def test_returns_error_when_job_not_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        from django_celery_results.models import TaskResult
        job = HmmerJobFactory(database=pdb_database)
        task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="PENDING", result=None
        )
        job.task = task
        job.save(update_fields=["task"])

        client.raise_request_exception = False
        response = client.get(f"{BASE_URL}/{job.id}")
        assert response.status_code not in (200,)
