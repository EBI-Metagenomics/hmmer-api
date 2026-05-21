import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def django_db_setup(postgres_container, redis_container, django_test_environment, django_db_blocker):
    from django.conf import settings
    from django.db import connections

    settings.DATABASES["default"] = {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": postgres_container.dbname,
        "USER": postgres_container.username,
        "PASSWORD": postgres_container.password,
        "HOST": postgres_container.get_container_host_ip(),
        "PORT": postgres_container.get_exposed_port(5432),
        "TEST": {"NAME": postgres_container.dbname},
    }

    # ConnectionHandler has no public API to swap databases at runtime; we clear
    # the cached 'settings' property and rewrite _settings directly. This relies
    # on Django internals (tested against Django 5.2).
    if "settings" in connections.__dict__:
        del connections.__dict__["settings"]
    connections._settings = connections.configure_settings(settings.DATABASES)
    connections.close_all()
    for alias in settings.DATABASES:
        connections[alias] = connections.create_connection(alias)

    redis_url = (
        f"redis://{redis_container.get_container_host_ip()}"
        f":{redis_container.get_exposed_port(6379)}"
    )
    settings.CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": redis_url,
        }
    }
    settings.CELERY_BROKER_URL = redis_url

    with django_db_blocker.unblock():
        from django.core.management import call_command
        call_command("migrate", verbosity=0)


@pytest.fixture(autouse=True)
def celery_eager(request, settings):
    """Run tasks in-process by default. Worker tests opt out via @pytest.mark.worker."""
    if "worker" not in request.keywords:
        settings.CELERY_TASK_ALWAYS_EAGER = True
        settings.CELERY_TASK_EAGER_PROPAGATES = True
        settings.CELERY_TASK_STORE_EAGER_RESULT = True


@pytest.fixture
def hits_bin_path():
    return Path(__file__).parent / "result" / "tests" / "data" / "hits.bin"


@pytest.fixture
def pdb_db_config():
    from hmmerapi.config import DatabaseSettings
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


@pytest.fixture
def hmmer_pdb_settings(settings, pdb_db_config):
    settings.HMMER.databases["pdb"] = pdb_db_config
    settings.HMMER.databases["pfam"] = pdb_db_config


@pytest.fixture
def mock_client_with_hits(hits_bin_path):
    """
    Patches search.tasks.Client and architecture.tasks.Client.
    When client.search(path=...) is called, copies hits.bin to that path.
    When called without path (build_annotation), returns the raw bytes.
    """
    hits_bytes = hits_bin_path.read_bytes()

    def _mock_search(db_cmd, parameters, query, path=None, ranges=None):
        if path is not None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(hits_bin_path, path)
        else:
            return hits_bytes

    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    mock.search.side_effect = _mock_search

    with patch("search.tasks.Client", return_value=mock):
        with patch("architecture.tasks.Client", return_value=mock):
            yield mock


@pytest.fixture
def tmp_storage():
    """
    Redirects results and downloads storage to a short /tmp path.

    result_path on HmmerJob is FilePathField(max_length=100) so we need a
    short base path — pytest's tmp_path on macOS exceeds 100 chars.
    """
    import tempfile
    import shutil as _shutil
    from django.core.files.storage import storages

    base = Path(tempfile.mkdtemp(prefix="ht_", dir="/tmp"))
    results_dir = base / "r"
    downloads_dir = base / "d"
    results_dir.mkdir()
    downloads_dir.mkdir()

    results_storage = storages["results"]
    downloads_storage = storages["downloads"]

    original_results = results_storage.location
    original_downloads = downloads_storage.location

    results_storage.location = str(results_dir)
    downloads_storage.location = str(downloads_dir)

    yield base

    results_storage.location = original_results
    downloads_storage.location = original_downloads
    _shutil.rmtree(base, ignore_errors=True)


def apply_task(task, args=None, kwargs=None):
    """
    Apply a Celery task in eager mode while pre-creating the TaskResult.

    Two eager-mode limitations require workarounds:
    1. mark_as_started() is skipped (track_started = not eager), so the
       TaskResult doesn't exist when the task body calls
       TaskResult.objects.get(task_id=self.request.id). We pre-create it.
    2. publish_result is False unless store_eager_result=True, so the backend
       never updates the status to SUCCESS. We set that flag temporarily.
    """
    import uuid
    from django_celery_results.models import TaskResult

    task_id = str(uuid.uuid4())
    TaskResult.objects.create(task_id=task_id, status="STARTED")

    original_store = getattr(task, "store_eager_result", None)
    task.store_eager_result = True
    try:
        return task.apply(args=args or [], kwargs=kwargs or {}, task_id=task_id)
    finally:
        if original_store is None:
            try:
                del task.store_eager_result
            except AttributeError:
                pass
        else:
            task.store_eager_result = original_store


@pytest.fixture
def pdb_database(db):
    from search.models import Database
    return Database.objects.create(
        id="pdb",
        name="PDB",
        version="2024_01",
        status="enabled",
        type="seq",
        order=1,
    )
