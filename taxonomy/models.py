from typing import List
from django.db import models
from django.core import serializers
from django.core.files.storage import storages
from treebeard.mp_tree import MP_Node, get_result_class
from django_celery_results.models import TaskResult
from result.models import Hit
from search.models import HmmerJob


class Taxonomy(MP_Node):
    taxonomy_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    rank = models.CharField(max_length=255)

    class Meta:
        indexes = [models.Index(fields=["taxonomy_id"])]

    @classmethod
    def dump(cls):
        """Dumps a tree branch to a python data structure."""

        cls = get_result_class(cls)

        qset = cls._get_serializable_model().objects.filter(range__isnull=False).distinct().order_by("depth", "path")
        ret, lnk = [], {}

        pk_field = cls._meta.pk.attname
        for pyobj in serializers.serialize("python", qset):

            fields = pyobj["fields"]
            path = fields["path"]
            depth = int(len(path) / cls.steplen)

            del fields["depth"]
            del fields["path"]
            del fields["numchild"]

            if pk_field in fields:
                del fields[pk_field]

            newobj = fields

            if depth == 1:
                ret.append(newobj)
            else:
                parentpath = cls._get_basepath(path, depth - 1)
                parentobj = lnk[parentpath]
                if "children" not in parentobj:
                    parentobj["children"] = []
                parentobj["children"].append(newobj)
            lnk[path] = newobj
        return ret

    @classmethod
    def build_distribution_tree(cls, db: str, hits: List[Hit]):
        taxonomy_ids = set([hit.metadata.taxonomy_id for hit in hits])
        species = cls.objects.filter(range__database=db, taxonomy_id__in=taxonomy_ids).distinct().order_by("depth", "path")

class Range(models.Model):
    database = models.CharField(max_length=32)
    taxonomy = models.ForeignKey(Taxonomy, to_field="taxonomy_id", on_delete=models.SET_NULL, null=True, blank=True)
    start = models.IntegerField(null=True, blank=True)
    end = models.IntegerField(null=True, blank=True)


class TaxonomyJob(models.Model):
    task = models.OneToOneField(
        TaskResult,
        to_field="task_id",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="taxonomy_job",
    )

    tree = models.FileField(
        null=True,
        blank=True,
        upload_to="%Y/%m/%d",
        storage=storages["results"],
    )

    hmmer_job = models.ForeignKey(HmmerJob, on_delete=models.CASCADE, related_name="taxonomy_job")
