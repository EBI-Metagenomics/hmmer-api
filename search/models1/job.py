import uuid
from django.db import models
from django_celery_results.models import TaskResult


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
    task = models.OneToOneField(TaskResult, null=True, blank=True, on_delete=models.CASCADE)

    algo = models.CharField(max_length=16, choices=AlgoChoices.choices, default=AlgoChoices.PHMMER)
    database = models.CharField(max_length=32)
    sequence = models.TextField()

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
    mx = models.CharField(max_length=16, choices=MXChoices.choices, default=MXChoices.BLOSUM62)

    def get_hmmpgmd_db(self) -> str:
        raise NotImplementedError()

    def get_hmmpgmd_parameters(self) -> str:
        raise NotImplementedError()

    def get_hmmpgmd_query(self) -> str:
        raise NotImplementedError()

    class Meta:
        abstract = True
