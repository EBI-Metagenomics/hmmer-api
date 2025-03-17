from django.db import models


class SequenceQueryMixin(models.Model):
    sequence = models.TextField()

    def get_hmmpgmd_query(self):
        return self.sequence

    class Meta:
        abstract = True
