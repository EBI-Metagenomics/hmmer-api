import json
import io
import shutil
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from django.core.files.base import ContentFile
from django_celery_results.models import TaskResult
from pyhmmer.easel import MSAFile
from pyhmmer.plan7 import HMMFile

from conftest import apply_task
from search.models import Database, HmmerJob
from search.tasks import (
    index_hits,
    notify_on_job_completion,
    run_search,
    schedule_batch_jobs,
    schedule_next_iteration,
)
from search.tests.factories import DatabaseFactory, HmmerJobFactory
from utils.functions import seq_to_hmm


def _serialize_hmms(input_text):
    hmms = []
    with HMMFile(io.BytesIO(input_text.encode())) as fh:
        while (hmm := fh.read()) is not None:
            with io.BytesIO() as out:
                hmm.write(out, binary=False)
                hmms.append(out.getvalue().decode())
    return hmms


def _serialize_msas(input_text):
    msas = []
    with MSAFile(io.BytesIO(input_text.encode())) as fh:
        while (msa := fh.read()) is not None:
            with io.BytesIO() as out:
                msa.write(out, format=fh.format)
                msas.append(out.getvalue().decode())
    return msas


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

    def test_replaces_existing_child_and_preserves_root_relationship(self, db, settings, pdb_database, hmmer_pdb_settings, tmp_storage):
        settings.HMMER.jackhmmer_max_iterations = 9
        job = HmmerJobFactory(
            database=pdb_database,
            algo=HmmerJob.AlgoChoices.JACKHMMER,
        )
        first_child = job.add_child(
            instance=HmmerJob(
                database=pdb_database,
                algo=HmmerJob.AlgoChoices.JACKHMMER,
                input_type=HmmerJob.InputChoices.UUID,
                input=str(job.id),
            )
        )

        with patch("celery.canvas.Signature.delay"):
            schedule_next_iteration.apply(args=[str(job.id)])

        job.refresh_from_db()
        children = list(job.get_children())

        assert len(children) == 1
        assert children[0].id != first_child.id
        assert children[0].get_root().id == job.id
        assert children[0].get_parent().id == job.id


@pytest.mark.django_db(transaction=True)
class TestScheduleBatchJobs:
    def test_creates_child_jobs_under_batch_root_in_id_order(self, pdb_database, hmmer_pdb_settings):
        job = HmmerJobFactory(
            database=pdb_database,
            input_type=HmmerJob.InputChoices.MULTI_SEQUENCE,
            input=">seq_b second\nBBBB\n>seq_a\nAAAA\n",
        )

        with patch("celery.canvas.Signature.delay"):
            schedule_batch_jobs.apply(args=[str(job.id)])

        job.refresh_from_db()
        children = list(job.get_children())

        assert len(children) == 2
        assert [child.id for child in children] == sorted(child.id for child in children)
        assert [child.input_type for child in children] == [HmmerJob.InputChoices.SEQUENCE] * 2
        assert [child.get_root().id for child in children] == [job.id, job.id]
        assert [child.get_parent().id for child in children] == [job.id, job.id]
        assert {child.input for child in children} == {">seq_a\nAAAA", ">seq_b second\nBBBB"}

    def test_splits_multi_hmm_into_single_hmm_children(self, pdb_database, hmmer_pdb_settings):
        combined_hmms = (
            seq_to_hmm(">hmm_one\nACDEFGHIK\n")
            + "\n"
            + seq_to_hmm(">hmm_two\nLMNPQRSTV\n")
        )
        expected_hmms = _serialize_hmms(combined_hmms)
        job = HmmerJobFactory(
            database=pdb_database,
            input_type=HmmerJob.InputChoices.MULTI_HMM,
            input=combined_hmms,
        )

        with patch("celery.canvas.Signature.delay"):
            schedule_batch_jobs.apply(args=[str(job.id)])

        job.refresh_from_db()
        children = list(job.get_children())

        assert len(children) == 2
        assert [child.input_type for child in children] == [HmmerJob.InputChoices.HMM] * 2
        assert [child.get_root().id for child in children] == [job.id, job.id]
        assert [child.get_parent().id for child in children] == [job.id, job.id]
        assert {child.input for child in children} == set(expected_hmms)
        assert all(child.input.count("HMMER") == 1 for child in children)

    def test_splits_multi_msa_into_single_msa_children(self, pdb_database, hmmer_pdb_settings):
        combined_msas = """# STOCKHOLM 1.0
seq_b/1-4 ACDE
seq_b2/1-4 AC-E
//
# STOCKHOLM 1.0
seq_a/1-4 LMNP
seq_a2/1-4 LM-P
//
"""
        expected_msas = _serialize_msas(combined_msas)
        job = HmmerJobFactory(
            database=pdb_database,
            input_type=HmmerJob.InputChoices.MULTI_MSA,
            input=combined_msas,
        )

        with patch("celery.canvas.Signature.delay"):
            schedule_batch_jobs.apply(args=[str(job.id)])

        job.refresh_from_db()
        children = list(job.get_children())

        assert len(children) == 2
        assert [child.input_type for child in children] == [HmmerJob.InputChoices.MSA] * 2
        assert [child.get_root().id for child in children] == [job.id, job.id]
        assert [child.get_parent().id for child in children] == [job.id, job.id]
        assert {child.input for child in children} == set(expected_msas)
        assert all(child.input.count("# STOCKHOLM 1.0") == 1 for child in children)

    def test_rejects_non_batch_input_type(self, pdb_database, hmmer_pdb_settings):
        job = HmmerJobFactory(
            database=pdb_database,
            input_type=HmmerJob.InputChoices.SEQUENCE,
            input=">seq_a\nAAAA\n",
        )

        with pytest.raises(Exception, match="Cannot schedule batch job with input type 'sequence'"):
            schedule_batch_jobs.apply(args=[str(job.id)])


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
