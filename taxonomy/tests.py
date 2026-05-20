from django.test import TestCase
from taxonomy.models import Taxonomy, Range


class GetSeqdbRangesFromTaxonomyTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.root = Taxonomy.add_root(id=1, name="root", rank="no rank")

        cls.root = Taxonomy.objects.get(pk=cls.root.pk)
        cls.bacteria = cls.root.add_child(
            id=2, name="Bacteria", rank="superkingdom", parent=cls.root
        )

        cls.root = Taxonomy.objects.get(pk=cls.root.pk)
        cls.archaea = cls.root.add_child(
            id=2157, name="Archaea", rank="superkingdom", parent=cls.root
        )

        cls.root = Taxonomy.objects.get(pk=cls.root.pk)
        cls.eukaryota = cls.root.add_child(
            id=2759, name="Eukaryota", rank="superkingdom", parent=cls.root
        )

        # Create ranges for a test database
        cls.db = "uniprot"
        Range.objects.create(database=cls.db, taxonomy=cls.root, start=1, end=1000)
        Range.objects.create(database=cls.db, taxonomy=cls.bacteria, start=1, end=400)
        Range.objects.create(database=cls.db, taxonomy=cls.archaea, start=401, end=700)
        Range.objects.create(
            database=cls.db, taxonomy=cls.eukaryota, start=701, end=1000
        )

    def test_empty_include_and_exclude_returns_empty_string(self):
        result = Range.get_seqdb_ranges_from_taxonomy(self.db, include=[], exclude=[])
        self.assertEqual(result, "")

    def test_include_single_taxonomy(self):
        result = Range.get_seqdb_ranges_from_taxonomy(self.db, include=[2], exclude=[])
        self.assertEqual("--seqdb_ranges 1..400", result)

    def test_include_multiple_taxonomies(self):
        result = Range.get_seqdb_ranges_from_taxonomy(
            self.db, include=[2, 2157], exclude=[]
        )
        self.assertEqual("--seqdb_ranges 1..700", result)

    def test_exclude_from_root(self):
        result = Range.get_seqdb_ranges_from_taxonomy(self.db, include=[], exclude=[2])
        self.assertEqual("--seqdb_ranges 401..1000", result)

    def test_include_and_exclude(self):
        result = Range.get_seqdb_ranges_from_taxonomy(
            self.db, include=[1], exclude=[2, 2759]
        )
        self.assertEqual("--seqdb_ranges 401..700", result)

    def test_exclude_creates_disjoint_ranges(self):
        result = Range.get_seqdb_ranges_from_taxonomy(
            self.db, include=[], exclude=[2157]
        )
        self.assertEqual("--seqdb_ranges 1..400,701..1000", result)
