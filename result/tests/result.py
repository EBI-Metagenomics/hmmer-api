import os
import tempfile
from django.test import SimpleTestCase

from hmmerapi.config import DatabaseSettings
from result.models import Result, HitsIndex


class ResultTestCase(SimpleTestCase):
    def setUp(self):
        self.binary_file_path = os.path.join(os.path.dirname(__file__), "data/hits.bin")

        if not os.path.exists(self.binary_file_path):
            self.skipTest(f"Binary file not found: {self.binary_file_path}")

        self.db_config = DatabaseSettings.model_validate(
            {
                "name": "pdb",
                "host": "pdb-master",
                "port": 51371,
                "db": 1,
                "db_file_location": "/opt/data/hmmpgmd/db.hmmpgmd",
                "external_link_template": "https://www.ebi.ac.uk/pdbe/entry/pdb/{}",
                "taxonomy_link_template": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={}",
                "architecture_database": "pdb",
            }
        )

    def test_result_from_file(self):
        result, hit_count = Result.from_file(self.binary_file_path, db_conf=self.db_config)

        self.assertGreater(hit_count, 0)
        self.assertIsNotNone(result)

        self.assertEqual(result.stats.nhits, 644)
        self.assertEqual(result.stats.nincluded, 527)

        self.assertEqual(len(result.hits), 644)

    def test_result_from_file_partial(self):
        result, hit_count = Result.from_file(self.binary_file_path, start=0, end=10, db_conf=self.db_config)

        self.assertGreater(hit_count, 0)
        self.assertIsNotNone(result)

        self.assertEqual(result.stats.nhits, 644)
        self.assertEqual(result.stats.nincluded, 527)

        self.assertEqual(len(result.hits), 10)

    def test_result_to_data(self):
        result, _ = Result.from_file(self.binary_file_path, with_domains=True, with_metadata=False)

        data = Result.to_data(result)

        with open(self.binary_file_path, mode="rb") as fh:
            data_from_file = fh.read()

        self.assertCountEqual(data, data_from_file)
        self.assertEqual(data, data_from_file)

    def test_result_to_data_no_domains(self):
        result, _ = Result.from_file(self.binary_file_path, with_domains=False, with_metadata=False)

        with self.assertRaises(ValueError):
            Result.to_data(result)

    def test_result_to_data_partial(self):
        result, _ = Result.from_file(self.binary_file_path, with_domains=False, with_metadata=False, start=0, end=10)

        with self.assertRaises(ValueError):
            Result.to_data(result)

    def test_result_index(self):
        result, _ = Result.from_file(
            self.binary_file_path, with_domains=False, with_metadata=True, db_conf=self.db_config
        )

        index = HitsIndex(result)

        self.assertEqual(len(index.taxonomy_index), 105)
        self.assertEqual(len(index.taxonomy_index[1]), 644)

        self.assertEqual(len(index.architecture_index), 63)

    def test_result_index_write(self):
        result, _ = Result.from_file(
            self.binary_file_path, with_domains=False, with_metadata=True, db_conf=self.db_config
        )

        index = HitsIndex(result)

        with tempfile.TemporaryDirectory() as temp_dir:
            file = os.path.join(temp_dir, "index.pkl")
            index.to_file(file)
            index_from_file = HitsIndex.from_file(file)

        self.assertEqual(index, index_from_file)

    def test_result_index_architecture(self):
        result, _ = Result.from_file(
            self.binary_file_path, with_domains=False, with_metadata=True, db_conf=self.db_config
        )

        index = HitsIndex(result)

        offsets = index.get_offsets_for_architecture("PF00018.33")
        self.assertEqual(len(offsets), 252)

        offsets = index.get_offsets_for_architecture("PF00017.29 PF00102.32")
        self.assertEqual(len(offsets), 1)

    def test_result_index_taxonomy(self):
        result, _ = Result.from_file(
            self.binary_file_path, with_domains=False, with_metadata=True, db_conf=self.db_config
        )

        index = HitsIndex(result)

        offsets = index.get_offsets_for_taxonomy_ids([9606])
        self.assertEqual(len(offsets), 408)

        offsets = index.get_offsets_for_taxonomy_ids([1])
        self.assertEqual(len(offsets), 644)
        self.assertEqual(offsets, result.stats.hit_offsets)

        offsets = index.get_offsets_for_taxonomy_ids([1, 9606])
        self.assertEqual(len(offsets), 644)
