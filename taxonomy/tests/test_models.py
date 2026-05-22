import pytest

from taxonomy.models import Range, Taxonomy


@pytest.mark.django_db
class TestTaxonomyModelIntegration:
    def test_children_are_returned_in_id_order_after_refresh(self):
        root = Taxonomy.add_root(id=1, name="root", rank="no rank")
        root = Taxonomy.objects.get(pk=root.pk)

        root.add_child(id=9, name="Ninth", rank="species", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        root.add_child(id=2, name="Second", rank="species", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        root.add_child(id=7, name="Seventh", rank="species", parent=root)

        root.refresh_from_db()

        assert [child.id for child in root.get_children()] == [2, 7, 9]

    def test_seqdb_ranges_follow_taxonomy_ordered_children(self):
        root = Taxonomy.add_root(id=1, name="root", rank="no rank")
        root = Taxonomy.objects.get(pk=root.pk)

        child_nine = root.add_child(id=9, name="Ninth", rank="species", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        child_two = root.add_child(id=2, name="Second", rank="species", parent=root)
        root = Taxonomy.objects.get(pk=root.pk)
        child_seven = root.add_child(id=7, name="Seventh", rank="species", parent=root)
        root.refresh_from_db()

        Range.objects.create(database="uniprot", taxonomy=root, start=1, end=90)
        Range.objects.create(database="uniprot", taxonomy=child_two, start=1, end=30)
        Range.objects.create(database="uniprot", taxonomy=child_seven, start=31, end=60)
        Range.objects.create(database="uniprot", taxonomy=child_nine, start=61, end=90)

        assert [child.id for child in root.get_children()] == [2, 7, 9]
        assert (
            Range.get_seqdb_ranges_from_taxonomy("uniprot", include=[2, 7], exclude=[])
            == "--seqdb_ranges 1..60"
        )
