import pytest
from taxonomy.models import Taxonomy, Range

DB = "uniprot"


@pytest.fixture(scope="module")
def taxonomy_ranges(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        root = Taxonomy.add_root(id=1, name="root", rank="no rank")
        root = Taxonomy.objects.get(pk=root.pk)
        bacteria = root.add_child(id=2, name="Bacteria", rank="superkingdom", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        archaea = root.add_child(id=2157, name="Archaea", rank="superkingdom", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        eukaryota = root.add_child(id=2759, name="Eukaryota", rank="superkingdom", parent=root)

        Range.objects.create(database=DB, taxonomy=root, start=1, end=1000)
        Range.objects.create(database=DB, taxonomy=bacteria, start=1, end=400)
        Range.objects.create(database=DB, taxonomy=archaea, start=401, end=700)
        Range.objects.create(database=DB, taxonomy=eukaryota, start=701, end=1000)

        yield

        Range.objects.filter(database=DB).delete()
        Taxonomy.objects.all().delete()


@pytest.mark.django_db
class GetSeqdbRangesFromTaxonomyTests:
    @pytest.fixture(autouse=True)
    def use_taxonomy_ranges(self, taxonomy_ranges):
        pass

    def test_empty_include_and_exclude_returns_empty_string(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[], exclude=[])
        assert result == ""

    def test_include_single_taxonomy(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[2], exclude=[])
        assert result == "--seqdb_ranges 1..400"

    def test_include_multiple_taxonomies(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[2, 2157], exclude=[])
        assert result == "--seqdb_ranges 1..700"

    def test_exclude_from_root(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[], exclude=[2])
        assert result == "--seqdb_ranges 401..1000"

    def test_include_and_exclude(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[1], exclude=[2, 2759])
        assert result == "--seqdb_ranges 401..700"

    def test_exclude_creates_disjoint_ranges(self):
        result = Range.get_seqdb_ranges_from_taxonomy(DB, include=[], exclude=[2157])
        assert result == "--seqdb_ranges 1..400,701..1000"
