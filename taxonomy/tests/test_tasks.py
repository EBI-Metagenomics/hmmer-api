import json
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from django.core.files.base import ContentFile

from conftest import apply_task
from search.tests.factories import HmmerJobFactory
from taxonomy.tasks import build_taxonomy_tree, build_taxonomy_distribution_graph
from taxonomy.models import TaxonomyTree, TaxonomyDistributionGraph


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
class TestBuildTaxonomyTree:
    def test_creates_json_output_file(self, job_with_result, tmp_storage):
        mock_tree = TaxonomyTree(id=1, name="All", hitcount=0, hitdist=[], children=[])

        with patch("taxonomy.tasks.TaxonomyTree.from_result", return_value=mock_tree):
            apply_task(build_taxonomy_tree, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        assert job_with_result.taxonomy_tree_task is not None

        result_path = json.loads(job_with_result.taxonomy_tree_task.result)
        assert Path(result_path).exists()

    def test_json_file_is_valid_tree(self, job_with_result, tmp_storage):
        mock_tree = TaxonomyTree(id=1, name="All", hitcount=5, hitdist=[], children=[])

        with patch("taxonomy.tasks.TaxonomyTree.from_result", return_value=mock_tree):
            apply_task(build_taxonomy_tree, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        result_path = json.loads(job_with_result.taxonomy_tree_task.result)
        with open(result_path) as f:
            tree = json.load(f)
        assert tree["id"] == 1
        assert tree["name"] == "All"


@pytest.mark.django_db(transaction=True)
class TestBuildTaxonomyDistributionGraph:
    def test_creates_json_output_file(self, job_with_result, tmp_storage):
        mock_graph = {"data": [], "labels": [], "categories": [], "colors": []}

        with patch("taxonomy.tasks.TaxonomyDistributionGraph.from_result", return_value=mock_graph):
            apply_task(build_taxonomy_distribution_graph, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        assert job_with_result.taxonomy_distribution_graph_task is not None

    def test_updates_job_task_reference(self, job_with_result, tmp_storage):
        mock_graph = {"data": [], "labels": [], "categories": [], "colors": []}

        with patch("taxonomy.tasks.TaxonomyDistributionGraph.from_result", return_value=mock_graph):
            apply_task(build_taxonomy_distribution_graph, args=[str(job_with_result.id)])

        job_with_result.refresh_from_db()
        assert job_with_result.taxonomy_distribution_graph_task.status == "SUCCESS"
