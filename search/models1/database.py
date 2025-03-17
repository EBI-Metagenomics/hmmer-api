from django.conf import settings
from django.db import models


class SequenceDatabaseMixin(models.Model):
    database = models.CharField(max_length=20)

    def get_hmmpgmd_db(self):
        try:
            db_config = settings.HMMER.databases[self.database]
        except KeyError:
            raise ValueError(f"Database {self.database} not found in settings")

        return f"--seqdb {db_config.db}"

    class Meta:
        abstract = True
