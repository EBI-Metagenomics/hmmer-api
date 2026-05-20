import os
import tempfile
from pathlib import Path

import pytest

from hmmerapi.config import DatabaseSettings
from result.models import Result, HitsIndex

BINARY_FILE_PATH = Path(__file__).parent / "data" / "hits.bin"


@pytest.fixture
def db_config():
    return DatabaseSettings.model_validate(
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


@pytest.fixture(autouse=True)
def require_hits_bin():
    if not BINARY_FILE_PATH.exists():
        pytest.skip(f"Binary file not found: {BINARY_FILE_PATH}")


def test_result_from_file(db_config):
    result, hit_count = Result.from_file(str(BINARY_FILE_PATH), db_conf=db_config)

    assert hit_count > 0
    assert result is not None
    assert result.stats.nhits == 644
    assert result.stats.nincluded == 527
    assert len(result.hits) == 644


def test_result_from_file_partial(db_config):
    result, hit_count = Result.from_file(str(BINARY_FILE_PATH), start=0, end=10, db_conf=db_config)

    assert hit_count > 0
    assert result is not None
    assert result.stats.nhits == 644
    assert result.stats.nincluded == 527
    assert len(result.hits) == 10


def test_result_to_data():
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=True, with_metadata=False)
    data = Result.to_data(result)

    with open(BINARY_FILE_PATH, mode="rb") as fh:
        data_from_file = fh.read()

    assert data == data_from_file


def test_result_to_data_no_domains():
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=False)

    with pytest.raises(ValueError):
        Result.to_data(result)


def test_result_to_data_partial():
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=False, start=0, end=10)

    with pytest.raises(ValueError):
        Result.to_data(result)


def test_result_index(db_config):
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=True, db_conf=db_config)
    index = HitsIndex(result)

    assert len(index.taxonomy_index) == 105
    assert len(index.taxonomy_index[1]) == 644
    assert len(index.architecture_index) == 63


def test_result_index_write(db_config):
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=True, db_conf=db_config)
    index = HitsIndex(result)

    with tempfile.TemporaryDirectory() as temp_dir:
        file = os.path.join(temp_dir, "index.pkl")
        index.to_file(file)
        index_from_file = HitsIndex.from_file(file)

    assert index == index_from_file


def test_result_index_architecture(db_config):
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=True, db_conf=db_config)
    index = HitsIndex(result)

    offsets = index.get_offsets_for_architecture("PF00018.33")
    assert len(offsets) == 252

    offsets = index.get_offsets_for_architecture("PF00017.29 PF00102.32")
    assert len(offsets) == 1


def test_result_index_taxonomy(db_config):
    result, _ = Result.from_file(str(BINARY_FILE_PATH), with_domains=False, with_metadata=True, db_conf=db_config)
    index = HitsIndex(result)

    offsets = index.get_offsets_for_taxonomy_ids([9606])
    assert len(offsets) == 408

    offsets = index.get_offsets_for_taxonomy_ids([1])
    assert len(offsets) == 644
    assert offsets == result.stats.hit_offsets

    offsets = index.get_offsets_for_taxonomy_ids([1, 9606])
    assert len(offsets) == 644
