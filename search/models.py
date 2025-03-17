import uuid
import io
from django.conf import settings
from django.db import models
from django_celery_results.models import TaskResult
from pyhmmer.easel import MSAFile, Alphabet
from pyhmmer.plan7 import Builder, Background


class HmmerJob(models.Model):
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
    database = models.CharField(max_length=32)
    input = models.TextField()

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

    @property
    def hmmpgmd_db(self) -> str:
        try:
            db_config = settings.HMMER.databases[self.database]
        except KeyError:
            raise ValueError(f"Database {self.database} not found in settings")

        if self.algo != self.AlgoChoices.HMMSCAN:
            return f"--seqdb {db_config.db}"

        return "--hmmdb 1"

    @property
    def hmmpgmd_parameters(self) -> str:
        fields_to_exclude = [
            "id",
            "task",
            "taxonomy_distribution_task",
            "taxonomy_tree_task",
            "taxonomy_distribution_graph_task",
            "algo",
            "database",
            "input",
            "threshold",
            "with_taxonomy",
            "with_architecture",
        ]

        params = ""

        if self.threshold == self.ThresholdChoices.CUT_GA:
            params = "--cut_ga"

        params += " ".join(
            f"{'-' if field.name in ["E", "T"] else '--'}{field.name} {getattr(self, field.name)}"
            for field in HmmerJob._meta.get_fields()
            if field.name not in fields_to_exclude and getattr(self, field.name) is not None
        )

        return params

    @property
    def hmmpgmd_query(self) -> str:
        if self.algo == HmmerJob.AlgoChoices.PHMMER or self.algo == HmmerJob.AlgoChoices.HMMSCAN:
            return self.input

        if self.algo == HmmerJob.AlgoChoices.HMMSEARCH:
            try:
                alphabet = Alphabet.amino()

                with MSAFile(io.BytesIO(self.input.encode()), digital=True, alphabet=alphabet) as msa_fh:
                    msa = msa_fh.read()

                    if msa.name is None:
                        msa.name = b"Query"

                    builder = Builder(alphabet)
                    background = Background(alphabet)
                    hmm, _, _ = builder.build_msa(msa, background)

                    hmm_fh = io.BytesIO()
                    hmm.write(hmm_fh, binary=False)
                    bytes = hmm_fh.getvalue()

                    return bytes.decode()
            except ValueError:
                return self.input

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
