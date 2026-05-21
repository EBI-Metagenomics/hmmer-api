from pathlib import Path

import pytest

from architecture.models import Annotation, Region


class TestRegionModel:
    def test_jagged_start_style_when_model_does_not_start_at_1(self):
        region = Region(
            text="PF00001",
            model_length=200,
            model_start=10,
            model_end=200,
            start=5,
            end=100,
            ali_start=5,
            ali_end=100,
        )
        assert region.start_style == "jagged"

    def test_straight_styles_for_motif_type(self):
        region = Region(
            text="PF00001",
            model_length=200,
            model_start=1,
            model_end=200,
            start=5,
            end=100,
            ali_start=5,
            ali_end=100,
            metadata={"type": "Motif"},
        )
        assert region.start_style == "straight"
        assert region.end_style == "straight"

    def test_jagged_end_style_when_model_does_not_reach_end(self):
        region = Region(
            text="PF00001",
            model_length=200,
            model_start=1,
            model_end=150,
            start=5,
            end=100,
            ali_start=5,
            ali_end=100,
        )
        assert region.end_style == "jagged"

    def test_curved_styles_for_full_domain(self):
        region = Region(
            text="PF00001",
            model_length=200,
            model_start=1,
            model_end=200,
            start=5,
            end=100,
            ali_start=5,
            ali_end=100,
            metadata={"model_length": 200},
        )
        assert region.start_style == "curved"
        assert region.end_style == "curved"


class TestAnnotationFromResults:
    def test_from_results_returns_annotation_with_int_length_and_list_regions(self):
        from result.models import Result
        from pathlib import Path

        hits_bin = Path(__file__).parents[3] / "result" / "tests" / "data" / "hits.bin"
        if not hits_bin.exists():
            pytest.skip("hits.bin not found")

        from hmmerapi.config import DatabaseSettings
        db_config = DatabaseSettings.model_validate(
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
        result, _ = Result.from_file(str(hits_bin), db_conf=db_config, with_domains=True, with_metadata=True)
        annotation = Annotation.from_results(result)
        assert isinstance(annotation.length, int)
        assert isinstance(annotation.regions, list)
