import shutil
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from search.models import Database, HmmerJob
from search.tests.factories import DatabaseFactory, HmmerJobFactory, SAMPLE_FASTA


@pytest.mark.django_db
class TestDatabaseModel:
    def test_str_choices_available(self):
        assert Database.StatusChoices.ENABLED == "enabled"
        assert Database.StatusChoices.DISABLED == "disabled"
        assert Database.StatusChoices.PAUSED == "paused"

    def test_type_choices_available(self):
        assert Database.TypeChoices.SEQ == "seq"
        assert Database.TypeChoices.HMM == "hmm"

    def test_database_creation(self):
        db = DatabaseFactory()
        assert db.pk == "pdb"
        assert db.status == "enabled"


@pytest.mark.django_db
class TestHmmerJobModel:
    def test_hmmpgmd_db_uses_seqdb_for_phmmer(self, hmmer_pdb_settings):
        job = HmmerJobFactory()
        assert "--seqdb" in job.hmmpgmd_db
        assert "--hmmdb" not in job.hmmpgmd_db

    def test_hmmpgmd_db_uses_hmmdb_for_hmmscan(self, hmmer_pdb_settings):
        db = DatabaseFactory(id="pdb", type=Database.TypeChoices.HMM)
        job = HmmerJobFactory(database=db, algo=HmmerJob.AlgoChoices.HMMSCAN)
        assert job.hmmpgmd_db == "--hmmdb 1"

    def test_clean_nulls_bitscore_fields_when_evalue_threshold(self):
        job = HmmerJobFactory(threshold=HmmerJob.ThresholdChoices.EVALUE)
        job.full_clean()
        assert job.T is None
        assert job.domT is None
        assert job.E is not None

    def test_clean_nulls_evalue_fields_when_bitscore_threshold(self):
        job = HmmerJobFactory(threshold=HmmerJob.ThresholdChoices.BITSCORE)
        job.full_clean()
        assert job.E is None
        assert job.domE is None
        assert job.T is not None

    def test_is_batch_mode_false_for_phmmer(self):
        job = HmmerJobFactory(input_type=HmmerJob.InputChoices.SEQUENCE)
        assert job.is_batch_mode is False

    def test_is_batch_mode_true_for_multi_sequence(self):
        job = HmmerJobFactory(input_type=HmmerJob.InputChoices.MULTI_SEQUENCE)
        assert job.is_batch_mode is True

    def test_convergence_stats_none_for_phmmer(self):
        job = HmmerJobFactory(algo=HmmerJob.AlgoChoices.PHMMER)
        assert job.convergence_stats is None

    def test_convergence_stats_none_for_jackhmmer_root(self):
        job = HmmerJobFactory(algo=HmmerJob.AlgoChoices.JACKHMMER)
        assert job.convergence_stats is None

    def test_clone_resets_task_and_result_references(self):
        job = HmmerJobFactory(result_path="/tmp/some/path.bin")
        clone = job.clone()
        assert clone.id is None
        assert clone.task is None
        assert clone.result_path is None
        assert clone.email_address is None

    def test_get_workflow_phmmer_returns_chain(self, hmmer_pdb_settings):
        job = HmmerJobFactory()
        workflow = job.get_workflow()
        assert workflow is not None

    def test_get_workflow_multi_sequence_returns_batch_signature(self, hmmer_pdb_settings):
        job = HmmerJobFactory(input_type=HmmerJob.InputChoices.MULTI_SEQUENCE)
        workflow = job.get_workflow()
        assert workflow is not None

    def test_pre_delete_signal_removes_result_directory(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir="/tmp") as base:
            result_dir = Path(base) / "r"
            result_dir.mkdir()
            result_file = result_dir / "hits.bin"
            result_file.write_bytes(b"data")

            job = HmmerJobFactory(result_path=str(result_file))
            job.delete()

            assert not result_dir.exists()
