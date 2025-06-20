import os

from django.test import SimpleTestCase

from hmmerapi.config import DatabaseSettings
from result.models import Result


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
