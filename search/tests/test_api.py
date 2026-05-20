import json
import uuid

import pytest

from search.models import Database, HmmerJob
from search.tests.factories import SAMPLE_FASTA

BASE_URL = "/api/v1/search"


@pytest.mark.django_db
class TestGetDatabases:
    def test_returns_empty_list_when_no_databases(self, client):
        response = client.get(f"{BASE_URL}/databases")
        assert response.status_code == 200
        assert response.json() == []

    def test_returns_databases_in_order(self, client, pdb_database):
        db2 = Database.objects.create(id="uniprot", name="UniProt", version="2024", order=2)
        response = client.get(f"{BASE_URL}/databases")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["id"] == "pdb"
        assert data[1]["id"] == "uniprot"
        db2.delete()


@pytest.mark.django_db
class TestPostPhmmer:
    def _post(self, client, body):
        return client.post(
            f"{BASE_URL}/phmmer",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_creates_job_and_returns_200(self, client, pdb_database):
        response = self._post(client, {"input": SAMPLE_FASTA, "database": "pdb"})
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert HmmerJob.objects.filter(id=data["id"]).exists()

    def test_job_stored_in_session(self, client, pdb_database):
        response = self._post(client, {"input": SAMPLE_FASTA, "database": "pdb"})
        assert response.status_code == 200
        job_id = response.json()["id"]
        session = client.session
        assert job_id in session.get("job_ids", [])

    def test_invalid_sequence_returns_422(self, client, pdb_database):
        response = self._post(client, {"input": "not_a_fasta_sequence!!!", "database": "pdb"})
        assert response.status_code == 422

    def test_missing_database_returns_422(self, client):
        response = self._post(client, {"input": SAMPLE_FASTA})
        assert response.status_code == 422

    def test_unknown_database_id_returns_422(self, client):
        # full_clean() calls ForeignKey.validate() which checks DB existence
        response = self._post(client, {"input": SAMPLE_FASTA, "database": "nonexistent_db"})
        assert response.status_code == 422

    def test_algo_stored_as_phmmer(self, client, pdb_database):
        response = self._post(client, {"input": SAMPLE_FASTA, "database": "pdb"})
        assert response.status_code == 200
        job = HmmerJob.objects.get(id=response.json()["id"])
        assert job.algo == HmmerJob.AlgoChoices.PHMMER


@pytest.mark.django_db
class TestPostHmmscan:
    def test_creates_job(self, client, pdb_database):
        response = client.post(
            f"{BASE_URL}/hmmscan",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        assert response.status_code == 200
        job = HmmerJob.objects.get(id=response.json()["id"])
        assert job.algo == HmmerJob.AlgoChoices.HMMSCAN


@pytest.mark.django_db
class TestPostJackhmmer:
    def test_creates_job(self, client, pdb_database):
        response = client.post(
            f"{BASE_URL}/jackhmmer",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        assert response.status_code == 200
        job = HmmerJob.objects.get(id=response.json()["id"])
        assert job.algo == HmmerJob.AlgoChoices.JACKHMMER

    def test_multi_sequence_input_rejected(self, client, pdb_database):
        multi = SAMPLE_FASTA + ">seq2\nACDEFGHIKLMNPQRSTVWY\n"
        response = client.post(
            f"{BASE_URL}/jackhmmer",
            data=json.dumps({"input": multi, "database": "pdb"}),
            content_type="application/json",
        )
        assert response.status_code == 422


@pytest.mark.django_db
class TestGetJobDetails:
    def test_returns_404_for_unknown_id(self, client):
        response = client.get(f"{BASE_URL}/{uuid.uuid4()}")
        assert response.status_code == 404

    def test_returns_job_details(self, client, pdb_database):
        response = client.post(
            "/api/v1/search/phmmer",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        job_id = response.json()["id"]
        response = client.get(f"{BASE_URL}/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == job_id
        assert data["algo"] == "phmmer"
        assert "result_path" not in data
        assert "hits_index_path" not in data


@pytest.mark.django_db
class TestPatchJob:
    def test_sets_email_address(self, client, pdb_database):
        response = client.post(
            "/api/v1/search/phmmer",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        job_id = response.json()["id"]

        patch_response = client.patch(
            f"{BASE_URL}/{job_id}",
            data=json.dumps({"email_address": "test@example.com"}),
            content_type="application/json",
        )
        assert patch_response.status_code == 204

        job = HmmerJob.objects.get(id=job_id)
        assert job.email_address == "test@example.com"

    def test_invalid_email_returns_422(self, client, pdb_database):
        response = client.post(
            "/api/v1/search/phmmer",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        job_id = response.json()["id"]

        patch_response = client.patch(
            f"{BASE_URL}/{job_id}",
            data=json.dumps({"email_address": "not-an-email"}),
            content_type="application/json",
        )
        assert patch_response.status_code == 422


@pytest.mark.django_db
class TestGetJobs:
    def test_returns_only_session_jobs(self, client, pdb_database):
        client.post(
            "/api/v1/search/phmmer",
            data=json.dumps({"input": SAMPLE_FASTA, "database": "pdb"}),
            content_type="application/json",
        )
        response = client.get(f"{BASE_URL}")
        assert response.status_code == 200
        assert len(response.json()) == 1
