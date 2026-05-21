import factory
from factory.django import DjangoModelFactory

from search.models import Database, HmmerJob

SAMPLE_FASTA = ">test_seq\nMKTLLLTLVVVTIVLAGHLGSAFSSYTTEETGNHITMEHFLSQLYEDSGDGRMIMKATTL\n"


class DatabaseFactory(DjangoModelFactory):
    class Meta:
        model = Database
        django_get_or_create = ("id",)

    id = "pdb"
    name = "PDB"
    version = "2024_01"
    status = Database.StatusChoices.ENABLED
    type = Database.TypeChoices.SEQ
    order = 1


class HmmerJobFactory(DjangoModelFactory):
    class Meta:
        model = HmmerJob

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        parent = kwargs.pop("_parent", None)
        if parent is not None:
            parent.refresh_from_db()
            return parent.add_child(instance=model_class(**kwargs))
        return model_class.add_root(**kwargs)

    algo = HmmerJob.AlgoChoices.PHMMER
    database = factory.SubFactory(DatabaseFactory)
    input = SAMPLE_FASTA
    input_type = HmmerJob.InputChoices.SEQUENCE
