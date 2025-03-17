# from django.conf import settings
# from django.db import models
# from search.models.job import HmmerJob


# class PhmmerJob(HmmerJob):
#     class MXEnum(models.TextChoices):
#         BLOSUM62 = "BLOSUM62"
#         BLOSUM45 = "BLOSUM45"
#         BLOSUM90 = "BLOSUM90"
#         PAM30 = "PAM30"
#         PAM70 = "PAM70"
#         PAM250 = "PAM250"

#     database = models.CharField(max_length=32)
#     sequence = models.TextField()

#     E = models.FloatField(default=1.0, null=True, blank=True)
#     domE = models.FloatField(default=1.0, null=True, blank=True)
#     T = models.FloatField(default=7.0, null=True, blank=True)
#     domT = models.FloatField(default=5.0, null=True, blank=True)
#     incE = models.FloatField(default=0.01, null=True, blank=True)
#     incdomE = models.FloatField(default=0.03, null=True, blank=True)
#     incT = models.FloatField(default=25.0, null=True, blank=True)
#     incdomT = models.FloatField(default=22.0, null=True, blank=True)
#     popen = models.FloatField(default=0.02, null=True, blank=True)
#     pextend = models.FloatField(default=0.4, null=True, blank=True)
#     mx = models.CharField(max_length=16, choices=MXEnum.choices, default=MXEnum.BLOSUM62)

#     def get_hmmpgmd_db(self):
#         [db_config] = [config for config in settings.HMMER.databases if config.name == self.database]

#         if db_config is None:
#             raise ValueError(f"No config found for {self.database}")

#         return f"--seqdb {db_config.db}"

#     def get_hmmpgmd_parameters(self):
#         parameters_to_exclude = ["threshold"]

#         return " ".join(f"-{key} {value}" for key, value in self.params.items() if key not in parameters_to_exclude)

#     def get_hmmpgmd_query(self):
#         return self.sequence
