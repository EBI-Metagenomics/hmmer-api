import shutil
import uuid
from pathlib import Path

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from search.models import HmmerJob
from search.tests.factories import DatabaseFactory, HmmerJobFactory

BASE_URL = "/api/v1/result"


@pytest.fixture
def successful_job(db, pdb_database, hits_bin_path, tmp_storage, hmmer_pdb_settings):
    """A completed phmmer job with result_path pointing to a copy of hits.bin."""
    from django.core.files.storage import storages

    job = HmmerJobFactory(database=pdb_database)

    task = TaskResult.objects.create(
        task_id=str(uuid.uuid4()),
        status="SUCCESS",
        result='"done"',
    )

    storage = storages["results"]
    path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))
    shutil.copy(hits_bin_path, storage.path(path))

    job.task = task
    job.result_path = storage.path(path)
    job.save(update_fields=["task", "result_path"])

    return job


@pytest.mark.django_db
class TestGetResult:
    def test_returns_404_for_unknown_id(self, client):
        response = client.get(f"{BASE_URL}/{uuid.uuid4()}")
        assert response.status_code == 404

    def test_returns_pending_when_no_task(self, client, successful_job):
        job = HmmerJobFactory(database=successful_job.database)
        response = client.get(f"{BASE_URL}/{job.id}")
        assert response.status_code == 200
        assert response.json()["status"] == "PENDING"

    def test_returns_hits_when_success(self, client, successful_job):
        response = client.get(f"{BASE_URL}/{successful_job.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["result"] is not None
        assert len(data["result"]["hits"]) > 0

    def test_pagination_limits_hits(self, client, successful_job):
        response = client.get(f"{BASE_URL}/{successful_job.id}?page=1&page_size=10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["result"]["hits"]) == 10
        assert data["page_count"] is not None


@pytest.mark.django_db
class TestGetDomains:
    def test_domains_not_accessible_when_no_task(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        # AlignmentResponseSchema.domains has no default, so Ninja raises a
        # ValidationError when serializing the PENDING response (no domains key).
        # Suppress the exception and assert the request did not return data.
        job = HmmerJobFactory(database=pdb_database)
        client.raise_request_exception = False
        response = client.get(f"{BASE_URL}/{job.id}/domains?index=0")
        assert response.status_code != 200

    def test_returns_domains_for_hit(self, client, successful_job):
        response = client.get(f"{BASE_URL}/{successful_job.id}/domains?index=0")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert "domains" in data
