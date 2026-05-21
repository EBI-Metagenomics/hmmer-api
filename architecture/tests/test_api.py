import json
import shutil
import uuid
from pathlib import Path

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from search.tests.factories import HmmerJobFactory

BASE_URL = "/api/v1/architecture"


@pytest.fixture
def job_with_tasks(db, pdb_database, hits_bin_path, tmp_storage, hmmer_pdb_settings):
    from django.core.files.storage import storages

    job = HmmerJobFactory(database=pdb_database)

    search_task = TaskResult.objects.create(
        task_id=str(uuid.uuid4()), status="SUCCESS", result='"done"'
    )

    storage = storages["results"]
    result_path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))
    shutil.copy(hits_bin_path, storage.path(result_path))

    arch_data = json.dumps([{"architecture": {"names": "PF00001", "score": 100, "graphics": {}, "accessions": "PF00001"}, "count": 5}])
    arch_path = storage.save(f"{job.id}/architecture.json", ContentFile(arch_data.encode()))

    arch_task = TaskResult.objects.create(
        task_id=str(uuid.uuid4()), status="SUCCESS", result=json.dumps(storage.path(arch_path))
    )

    job.task = search_task
    job.result_path = storage.path(result_path)
    job.architecture_task = arch_task
    job.save(update_fields=["task", "result_path", "architecture_task"])

    return job


@pytest.mark.django_db
class TestGetDomainArchitectures:
    def test_returns_pending_when_search_not_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        job = HmmerJobFactory(database=pdb_database)
        response = client.get(f"{BASE_URL}/{job.id}")
        assert response.status_code == 200
        assert response.json()["status"] == "PENDING"

    def test_returns_architectures_when_done(self, client, job_with_tasks):
        response = client.get(f"{BASE_URL}/{job_with_tasks.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["architectures"] is not None

    def test_pagination_works(self, client, job_with_tasks):
        response = client.get(f"{BASE_URL}/{job_with_tasks.id}?page=1&page_size=1")
        assert response.status_code == 200
        data = response.json()
        assert data["page_count"] is not None


@pytest.mark.django_db
class TestGetAnnotations:
    def test_returns_pending_when_not_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        job = HmmerJobFactory(database=pdb_database)
        response = client.get(f"{BASE_URL}/{job.id}/annotations")
        assert response.status_code == 200
        assert response.json()["status"] == "PENDING"

    def test_returns_annotations_when_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        from django.core.files.storage import storages
        from architecture.models import Annotation

        job = HmmerJobFactory(database=pdb_database)
        search_task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="SUCCESS", result='"done"'
        )
        storage = storages["results"]
        annotation_data = Annotation(length=100, regions=[]).model_dump_json()
        ann_path = storage.save(f"{job.id}/annotation.json", ContentFile(annotation_data.encode()))

        ann_task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="SUCCESS", result=json.dumps(storage.path(ann_path))
        )
        job.task = search_task
        job.annotation_task = ann_task
        job.save(update_fields=["task", "annotation_task"])

        response = client.get(f"{BASE_URL}/{job.id}/annotations")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["annotations"] is not None
