import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from django.core.files.base import ContentFile

from conftest import apply_task
from architecture.tasks import build_architecture, build_annotation
from search.tests.factories import HmmerJobFactory


@pytest.fixture
def job_with_result(db, pdb_database, hits_bin_path, tmp_storage, hmmer_pdb_settings):
    from django.core.files.storage import storages

    job = HmmerJobFactory(database=pdb_database)
    storage = storages["results"]
    path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))
    shutil.copy(hits_bin_path, storage.path(path))
    job.result_path = storage.path(path)
    job.save(update_fields=["result_path"])
    return job


@pytest.mark.django_db(transaction=True)
class TestBuildArchitecture:
    def test_creates_architecture_json(self, job_with_result, tmp_storage):
        with patch("architecture.tasks.Architecture.from_raw_hits", return_value=[]):
            apply_task(build_architecture, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        assert job_with_result.architecture_task is not None
        result_path = json.loads(job_with_result.architecture_task.result)
        assert Path(result_path).exists()

    def test_updates_job_architecture_task(self, job_with_result, tmp_storage):
        with patch("architecture.tasks.Architecture.from_raw_hits", return_value=[]):
            apply_task(build_architecture, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        assert job_with_result.architecture_task.status == "SUCCESS"

    def test_json_file_is_valid_list(self, job_with_result, tmp_storage):
        mock_data = [{"architecture": {"names": "PF00001"}, "count": 5}]
        with patch("architecture.tasks.Architecture.from_raw_hits", return_value=mock_data):
            apply_task(build_architecture, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        result_path = json.loads(job_with_result.architecture_task.result)
        with open(result_path) as f:
            data = json.load(f)
        assert isinstance(data, list)


@pytest.mark.django_db(transaction=True)
class TestBuildAnnotation:
    def _apply_with_mocked_result(self, job_with_result):
        from architecture.models import Annotation
        mock_annotation = Annotation(length=0, regions=[])
        with patch("architecture.tasks.Result.from_data", return_value=(object(), None)):
            with patch("architecture.tasks.post_process_pfam"):
                with patch("architecture.tasks.predict_active_sites"):
                    with patch("architecture.tasks.Annotation.from_results", return_value=mock_annotation):
                        return apply_task(build_annotation, args=[str(job_with_result.id)])

    def test_creates_annotation_json(self, job_with_result, mock_client_with_hits, tmp_storage, hmmer_pdb_settings):
        self._apply_with_mocked_result(job_with_result)

        job_with_result.refresh_from_db()
        assert job_with_result.annotation_task is not None
        result_path = json.loads(job_with_result.annotation_task.result)
        assert Path(result_path).exists()

    def test_updates_job_annotation_task(self, job_with_result, mock_client_with_hits, tmp_storage, hmmer_pdb_settings):
        self._apply_with_mocked_result(job_with_result)

        job_with_result.refresh_from_db()
        assert job_with_result.annotation_task.status == "SUCCESS"
