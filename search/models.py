import uuid
import datetime
import os
import io
import concurrent.futures
import shutil
import logging

from pathlib import Path
from typing import TextIO, Union, List
from django.conf import settings
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django_celery_results.models import TaskResult
from celery import signature, chain, group, states
from pyhmmer.easel import SSIReader, TextSequence, TextSequenceBlock, SequenceFile, MSAFile
from pyhmmer.plan7 import HMMFile
from treebeard.al_tree import AL_Node

from result.models import Result, Restrictions, P7HitFlags, HmmdSearchStats
from utils.functions import seq_to_hmm, msa_to_hmm, hmm_from_hmmpgmd

logger = logging.getLogger(__name__)


class HmmerJob(AL_Node):
    class MXChoices(models.TextChoices):
        BLOSUM62 = "BLOSUM62"
        BLOSUM45 = "BLOSUM45"
        BLOSUM90 = "BLOSUM90"
        PAM30 = "PAM30"
        PAM70 = "PAM70"
        PAM250 = "PAM250"

    class AlgoChoices(models.TextChoices):
        PHMMER = "phmmer"
        HMMSEARCH = "hmmsearch"
        HMMSCAN = "hmmscan"
        JACKHMMER = "jackhmmer"

    class ThresholdChoices(models.TextChoices):
        EVALUE = "evalue"
        BITSCORE = "bitscore"
        CUT_GA = "cut_ga"

    class InputChoices(models.TextChoices):
        SEQUENCE = "sequence"
        HMM = "hmm"
        MSA = "msa"
        UUID = "uuid"
        MULTI_SEQUENCE = "multi_sequence"
        MULTI_HMM = "multi_hmm"
        MULTI_MSA = "multi_msa"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, unique=True)
    task = models.OneToOneField(TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE)
    taxonomy_distribution_task = models.OneToOneField(
        TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE
    )
    taxonomy_tree_task = models.OneToOneField(
        TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE
    )
    taxonomy_distribution_graph_task = models.OneToOneField(
        TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE
    )
    architecture_task = models.OneToOneField(
        TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE
    )
    annotation_task = models.OneToOneField(
        TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE
    )

    algo = models.CharField(max_length=16, choices=AlgoChoices.choices, default=AlgoChoices.PHMMER)
    database = models.ForeignKey("Database", on_delete=models.SET_NULL, related_name="+", null=True, blank=True)
    input = models.TextField(null=True, blank=True)
    input_type = models.CharField(max_length=16, choices=InputChoices.choices, default=InputChoices.SEQUENCE)
    calculated_input = models.TextField(null=True, blank=True)
    result_path = models.FilePathField(path=settings.HMMER.results_storage_location, null=True, blank=True)
    hits_index_path = models.FilePathField(path=settings.HMMER.results_storage_location, null=True, blank=True)

    threshold = models.CharField(max_length=16, choices=ThresholdChoices.choices, default=ThresholdChoices.EVALUE)
    E = models.FloatField(default=1.0, null=True, blank=True)
    domE = models.FloatField(default=1.0, null=True, blank=True)
    T = models.FloatField(default=7.0, null=True, blank=True)
    domT = models.FloatField(default=5.0, null=True, blank=True)
    incE = models.FloatField(default=0.01, null=True, blank=True)
    incdomE = models.FloatField(default=0.03, null=True, blank=True)
    incT = models.FloatField(default=25.0, null=True, blank=True)
    incdomT = models.FloatField(default=22.0, null=True, blank=True)

    popen = models.FloatField(default=0.02, null=True, blank=True)
    pextend = models.FloatField(default=0.4, null=True, blank=True)
    mx = models.CharField(max_length=16, null=True, blank=True, choices=MXChoices.choices, default=MXChoices.BLOSUM62)

    with_taxonomy = models.BooleanField(default=False)
    with_architecture = models.BooleanField(default=False)

    include = models.JSONField(default=list)
    exclude = models.JSONField(default=list)
    exclude_all = models.BooleanField(default=False)
    iterations = models.IntegerField(null=True, blank=True)

    date_submitted = models.DateTimeField(auto_now_add=True, null=True)
    number_of_hits = models.IntegerField(null=True, blank=True)
    number_of_included = models.IntegerField(null=True, blank=True)
    number_of_gained = models.IntegerField(null=True, blank=True)
    number_of_dropped = models.IntegerField(null=True, blank=True)
    number_of_lost = models.IntegerField(null=True, blank=True)
    first_gained_index = models.IntegerField(null=True, blank=True)

    email_address = models.EmailField(null=True, blank=True, max_length=254)

    parent = models.ForeignKey("self", related_name="children_set", null=True, db_index=True, on_delete=models.CASCADE)
    node_order_by = ["id"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.restrictions = None

    @property
    def hmmpgmd_db(self) -> str:
        try:
            db_config = settings.HMMER.databases[self.database.id]
        except KeyError:
            raise ValueError(f"Database {self.database.id} not found in settings")

        if self.algo != self.AlgoChoices.HMMSCAN:
            return f"--seqdb {db_config.db}"

        return "--hmmdb 1"

    @property
    def hmmpgmd_parameters(self) -> str:
        fields_to_include = [
            "E",
            "domE",
            "T",
            "domT",
            "incE",
            "incdomE",
            "incT",
            "incdomT",
            "popen",
            "pextend",
            "mx",
        ]

        params = ""

        if self.threshold == self.ThresholdChoices.CUT_GA:
            params = "--cut_ga"

        params += " ".join(
            f"{'-' if field.name in ["E", "T"] else '--'}{field.name} {getattr(self, field.name)}"
            for field in HmmerJob._meta.get_fields()
            if field.name in fields_to_include and getattr(self, field.name) is not None
        )

        return params

    @property
    def hmmpgmd_query(self) -> str:
        if self.algo == HmmerJob.AlgoChoices.PHMMER or self.algo == HmmerJob.AlgoChoices.HMMSCAN:
            return self.input

        if self.algo == HmmerJob.AlgoChoices.HMMSEARCH:
            if self.input_type == HmmerJob.InputChoices.HMM:
                return self.input
            else:
                return msa_to_hmm(self.input)

        if self.algo == HmmerJob.AlgoChoices.JACKHMMER:
            if self.input_type == HmmerJob.InputChoices.HMM:
                return self.input
            elif self.input_type == HmmerJob.InputChoices.SEQUENCE:
                return seq_to_hmm(self.input)
            elif self.input_type == HmmerJob.InputChoices.MSA:
                return msa_to_hmm(self.input)
            else:
                input_job = HmmerJob.objects.get(id=self.input)

                if self.iteration == 1:
                    query = input_job.hmmpgmd_query
                else:
                    query = hmm_from_hmmpgmd(
                        input_job.result_path,
                        input_job.calculated_input,
                        input_job.include,
                        input_job.exclude,
                        input_job.exclude_all,
                    )

                self.calculated_input = query
                self.save(update_fields=["calculated_input"])

                return query

    @property
    def input_hmm(self) -> str:
        if self.input_type == HmmerJob.InputChoices.HMM:
            return self.input
        elif self.input_type == HmmerJob.InputChoices.SEQUENCE:
            return seq_to_hmm(self.input)
        elif self.input_type == HmmerJob.InputChoices.MSA:
            return msa_to_hmm(self.input)
        else:
            return self.calculated_input

    @property
    def iteration(self):
        if self.algo != self.AlgoChoices.JACKHMMER:
            return None

        return self.get_depth() - 1

    @property
    def convergence_stats(self):
        if self.algo != self.AlgoChoices.JACKHMMER:
            return None

        if self.iteration == 0:
            return None

        if self.task and self.task.status != "SUCCESS":
            return None

        return {
            "gained": self.number_of_gained,
            "dropped": self.number_of_dropped,
            "lost": self.number_of_lost,
            "total": self.number_of_included,
        }

    @property
    def previous_job_id(self):
        previous_job = self.get_parent()
        root_job = self.get_root()

        if previous_job and root_job.id != previous_job.id:
            return previous_job.id

        return None

    @property
    def next_job_id(self):
        next_job = self.get_first_child()

        if next_job:
            return next_job.id

        return None

    @property
    def parent_job_id(self):
        parent_job = self.get_root()

        if parent_job and parent_job.id != self.id:
            return parent_job.id

        return None

    @property
    def is_batch_mode(self):
        if self.algo == HmmerJob.AlgoChoices.JACKHMMER:
            return self.iterations is not None and self.iterations > 1

        return self.input_type in {
            HmmerJob.InputChoices.MULTI_SEQUENCE,
            HmmerJob.InputChoices.MULTI_MSA,
            HmmerJob.InputChoices.MULTI_HMM,
        }

    @property
    def query_name(self):
        if self.input_type == HmmerJob.InputChoices.MULTI_SEQUENCE:
            return "multiple sequences"

        if self.input_type == HmmerJob.InputChoices.MULTI_MSA:
            return "multiple MSAs"

        if self.input_type == HmmerJob.InputChoices.MULTI_HMM:
            return "multiple HMMs"

        if self.input_type == HmmerJob.InputChoices.SEQUENCE:
            try:
                with SequenceFile(io.BytesIO(self.input.encode()), format="fasta") as fh:
                    sequence = fh.read()
                    return sequence.name.decode()
            except ValueError:
                return "unknown"
        if self.input_type == HmmerJob.InputChoices.MSA:
            try:
                with MSAFile(io.BytesIO(self.input.encode())) as fh:
                    msa = fh.read()
                    if msa.name is not None:
                        return msa.name.decode()
                    else:
                        return "unnamed MSA"
            except ValueError:
                return "unknown"
        if self.input_type == HmmerJob.InputChoices.HMM:
            try:
                with HMMFile(io.BytesIO(self.input.encode())) as fh:
                    hmm = fh.read()
                    if hmm.name is not None:
                        return hmm.name.decode()
                    else:
                        return "unnamed HMM"
            except ValueError:
                return "unknown"

        return ""

    def set_restrictions(self, restrictions: Restrictions):
        self.restrictions = restrictions

    def get_result(self):
        if self.task is None or self.task.status != states.SUCCESS:
            return None

        try:
            db_conf = settings.HMMER.databases[self.database.id]
        except KeyError:
            raise ValueError(f"Database {self.database.id} not found in settings")

        result, total_count = Result.from_file(
            self.result_path,
            algo=self.algo,
            id=self.id,
            db_conf=db_conf,
            index_file=self.hits_index_path,
            **(self.restrictions.model_dump() or {}),
        )

        if self.algo == HmmerJob.AlgoChoices.JACKHMMER and self.iteration > 0:
            result.stats.ngained = self.number_of_gained
            result.stats.ndropped = self.number_of_dropped
            result.stats.nlost = self.number_of_lost
            result.stats.first_gained_index = self.first_gained_index

        return result, total_count

    def clean(self):
        super().clean()

        if self.threshold is None:
            if self.algo == self.AlgoChoices.HMMSCAN:
                self.threshold = self.ThresholdChoices.CUT_GA
            else:
                self.threshold = self.ThresholdChoices.EVALUE

        if self.threshold != self.ThresholdChoices.BITSCORE:
            self.T = None
            self.domT = None
            self.incT = None
            self.incdomT = None

        if self.threshold != self.ThresholdChoices.EVALUE:
            self.E = None
            self.domE = None
            self.incE = None
            self.incdomE = None

        if self.algo == self.AlgoChoices.HMMSCAN:
            self.mx = None
            self.popen = None
            self.pextend = None

    def get_workflow(self, as_batch=False):
        if self.algo == self.AlgoChoices.JACKHMMER:
            if self.iteration == 0:
                return signature("search.tasks.schedule_next_iteration", args=(self.id,), immutable=True)
            else:
                workflow = [signature("search.tasks.run_search", args=(self.id,), immutable=True)]

                subsequent_tasks = [signature("search.tasks.index_hits", args=(self.id,), immutable=True)]

                if self.with_taxonomy:
                    subsequent_tasks += [
                        signature("taxonomy.tasks.build_taxonomy_tree", args=(self.id,), immutable=True),
                        signature("taxonomy.tasks.build_taxonomy_distribution_graph", args=(self.id,), immutable=True),
                    ]

                if self.with_architecture:
                    subsequent_tasks.append(
                        signature("architecture.tasks.build_architecture", args=(self.id,), immutable=True)
                    )

                if self.is_batch_mode and self.iteration < self.iterations:
                    subsequent_tasks.append(
                        signature("search.tasks.schedule_next_iteration", args=(self.id,), immutable=True)
                    )

                if subsequent_tasks:
                    workflow.append(group(*subsequent_tasks))

                workflow_chain = chain(
                    *workflow,
                    signature("search.tasks.notify_on_job_completion", args=(self.id,), immutable=True),
                )

                workflow_chain.link_error(
                    signature("search.tasks.notify_on_job_completion", args=(self.id,), immutable=True)
                )

                return workflow_chain

        else:
            if self.is_batch_mode:
                return signature("search.tasks.schedule_batch_jobs", args=(self.id,), immutable=True)

            subsequent_tasks = []

            if self.algo != HmmerJob.AlgoChoices.HMMSCAN:
                subsequent_tasks.append(signature("search.tasks.index_hits", args=(self.id,), immutable=True))

            if self.algo != self.AlgoChoices.HMMSCAN and self.with_taxonomy:
                subsequent_tasks += [
                    signature("taxonomy.tasks.build_taxonomy_tree", args=(self.id,), immutable=True),
                    signature("taxonomy.tasks.build_taxonomy_distribution_graph", args=(self.id,), immutable=True),
                ]

            if self.algo != self.AlgoChoices.HMMSCAN and self.with_architecture:
                subsequent_tasks += [
                    signature("architecture.tasks.build_architecture", args=(self.id,), immutable=True)
                ]

            if self.algo != self.AlgoChoices.HMMSEARCH:
                subsequent_tasks += [signature("architecture.tasks.build_annotation", args=(self.id,), immutable=True)]

            workflow_chain = chain(
                signature(
                    "search.tasks.run_search",
                    args=(self.id,),
                    immutable=True,
                    queue="batch_queue" if as_batch else "io_bound_queue",
                ),
                group(*subsequent_tasks),
                signature("search.tasks.notify_on_job_completion", args=(self.id,), immutable=True),
            )

            workflow_chain.link_error(
                signature("search.tasks.notify_on_job_completion", args=(self.id,), immutable=True)
            )

            return workflow_chain

    def post_process(self):
        fields_to_update = []

        if self.algo != HmmerJob.AlgoChoices.JACKHMMER:
            stats = HmmdSearchStats.from_file(self.result_path)
            self.number_of_hits = stats.nreported
            self.number_of_included = stats.nincluded
            fields_to_update += ["number_of_hits", "number_of_included"]

        if self.algo == HmmerJob.AlgoChoices.JACKHMMER and self.iteration > 0:
            if self.iteration > 1:
                prev_job = self.get_parent()

                prev_result, _ = Result.from_file(prev_job.result_path, with_domains=False, with_metadata=False)
                prev_included_set = {int(hit.name) for hit in prev_result.hits if hit.is_included}
                del prev_result
            else:
                prev_included_set = set()

            result, _ = Result.from_file(self.result_path, with_domains=True, with_metadata=False)
            included_set = {int(hit.name) for hit in result.hits if hit.is_included}
            reported_set = {int(hit.name) for hit in result.hits if hit.is_reported}

            gained_set = included_set - prev_included_set
            dropped_set = prev_included_set & (reported_set - included_set)
            lost_set = prev_included_set - reported_set

            for hit in result.hits:
                if int(hit.name) in gained_set:
                    hit.flags = P7HitFlags(P7HitFlags["IS_NEW"] | P7HitFlags["IS_INCLUDED"] | P7HitFlags["IS_REPORTED"])
                    hit.is_new = True

                if int(hit.name) in dropped_set:
                    hit.flags = P7HitFlags(P7HitFlags["IS_DROPPED"] | P7HitFlags["IS_REPORTED"])
                    hit.is_dropped = True

            first_gained_hit = next((hit for hit in result.hits if hit.is_new), None)

            self.number_of_gained = len(gained_set)
            self.number_of_dropped = len(dropped_set)
            self.number_of_lost = len(lost_set)
            self.number_of_hits = result.stats.nreported
            self.number_of_included = result.stats.nincluded
            self.first_gained_index = first_gained_hit.index if first_gained_hit is not None else None

            Result.to_file(result, self.result_path)

            fields_to_update += [
                "number_of_gained",
                "number_of_dropped",
                "number_of_lost",
                "number_of_hits",
                "number_of_included",
                "first_gained_index",
            ]

        self.save(update_fields=fields_to_update)


@receiver(pre_delete, sender=HmmerJob)
def cleanup_hmmer_job_files(sender, instance: HmmerJob, **kwargs):
    if instance.task is None and instance.result_path is None:
        return

    try:
        enclosing_directory = Path(instance.result_path).parent
        shutil.rmtree(enclosing_directory, ignore_errors=True)
    except Exception as e:
        logger.warning(e)


class Database(models.Model):
    class TypeChoices(models.TextChoices):
        SEQ = "seq"
        HMM = "hmm"

    id = models.CharField(max_length=32, primary_key=True, unique=True)
    type = models.CharField(max_length=16, choices=TypeChoices.choices, default=TypeChoices.SEQ)
    name = models.CharField(max_length=32)
    version = models.CharField(max_length=32)
    release_date = models.DateField(default=datetime.date.today)
    order = models.IntegerField(default=-1)


class SequenceFetcher:
    def __init__(self, db_file: os.PathLike):
        self.db_file = db_file

    def _fetch_sequence(self, fh: TextIO, ssi_reader: SSIReader, key: Union[str, int]):
        entry = ssi_reader.find_name(str(key).encode())
        fh.seek(entry.data_offset)
        return fh.read(entry.record_length)

    def _fetch_sequence_chunk(self, keys: Union[List[str], List[int]]):
        with SSIReader(f"{self.db_file}.ssi") as ssi_reader, open(self.db_file, "rt") as fh:
            return {key: self._fetch_sequence(fh, ssi_reader, key) for key in keys}

    def fetch_sequences(self, keys: List[int]):
        max_workers = 4
        chunk_size = 1000
        chunks = [keys[i: i + chunk_size] for i in range(0, len(keys), chunk_size)]

        all_sequences: dict[str | int, str] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._fetch_sequence_chunk, chunk) for chunk in chunks]

            for future in concurrent.futures.as_completed(futures):
                all_sequences.update(future.result())

        return TextSequenceBlock([TextSequence(name=str(key).encode(), sequence=all_sequences[key]) for key in keys])
