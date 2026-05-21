import json
import shutil
import uuid
from pathlib import Path

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult

from search.models import HmmerJob
from search.tests.factories import DatabaseFactory, HmmerJobFactory
from taxonomy.models import Taxonomy, Range

BASE_URL = "/api/v1/taxonomy"


@pytest.fixture
def taxonomy_nodes(db):
    root = Taxonomy.add_root(id=1, name="root", rank="no rank")
    root = Taxonomy.objects.get(pk=root.pk)
    bacteria = root.add_child(id=2, name="Bacteria", rank="superkingdom", parent=root)
    Range.objects.create(database="pdb", taxonomy=root, start=1, end=1000)
    Range.objects.create(database="pdb", taxonomy=bacteria, start=1, end=400)
    return {"root": root, "bacteria": bacteria}


@pytest.mark.django_db
class TestSearchTaxonomy:
    def test_search_by_name_returns_matches(self, client, taxonomy_nodes):
        response = client.get(f"{BASE_URL}/search?q=Bacteria&database=pdb")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "Bacteria"

    def test_search_by_id_returns_match(self, client, taxonomy_nodes):
        response = client.get(f"{BASE_URL}/search?q=2&database=pdb")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 2

    def test_empty_query_returns_empty(self, client, taxonomy_nodes):
        response = client.get(f"{BASE_URL}/search?q=&database=pdb")
        assert response.status_code == 200
        assert response.json() == []


@pytest.mark.django_db
class TestGetTaxonomy:
    def test_returns_taxonomy_node(self, client, taxonomy_nodes):
        response = client.get(f"{BASE_URL}/2")
        assert response.status_code == 200
        assert response.json()["name"] == "Bacteria"

    def test_returns_404_for_unknown(self, client):
        response = client.get(f"{BASE_URL}/999999")
        assert response.status_code == 404


@pytest.mark.django_db
class TestGetTaxonomyTree:
    def test_returns_pending_when_not_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        job = HmmerJobFactory(database=pdb_database)
        response = client.get(f"{BASE_URL}/{job.id}/tree")
        assert response.status_code == 200
        assert response.json()["status"] == "PENDING"

    def test_returns_tree_when_done(self, client, pdb_database, tmp_storage, hmmer_pdb_settings):
        from django.core.files.storage import storages

        job = HmmerJobFactory(database=pdb_database)
        search_task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="SUCCESS", result='"done"'
        )
        job.task = search_task
        job.save(update_fields=["task"])

        storage = storages["results"]
        tree_data = json.dumps({"id": 1, "name": "All", "hitcount": 0, "hitdist": [], "children": []})
        path = storage.save(f"{job.id}/taxonomy_tree.json", ContentFile(b""))
        storage.open(path, mode="wt").write(tree_data)

        tree_path = storage.path(path)
        tree_task = TaskResult.objects.create(
            task_id=str(uuid.uuid4()), status="SUCCESS", result=json.dumps(tree_path)
        )
        job.taxonomy_tree_task = tree_task
        job.save(update_fields=["taxonomy_tree_task"])

        response = client.get(f"{BASE_URL}/{job.id}/tree")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["tree"]["id"] == 1
