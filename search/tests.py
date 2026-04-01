from unittest.mock import MagicMock, patch

from celery.contrib.testing.worker import start_worker
from django.conf import settings
from django.test import TransactionTestCase
from hmmerapi.celery import app as celery_app
from hmmerapi.config import DatabaseSettings

from search.models import Database, HmmerJob
from search.tasks import run_search


class RunSearchPausedTests(TransactionTestCase):
    """Worker-level tests: PAUSED database parks the job indefinitely."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._worker = start_worker(celery_app, concurrency=1, perform_ping_check=False, loglevel="critical")
        cls._worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._worker.__exit__(None, None, None)
        super().tearDownClass()

    def setUp(self):
        self.database = Database.objects.create(
            id="pdb",
            name="PDB",
            version="1.0",
            status=Database.StatusChoices.PAUSED,
        )
        self.job = HmmerJob.add_root(database=self.database)

    def _run_task(self):
        """Submit the task and intercept self.retry(), returning the mock."""
        with patch.object(run_search, "retry", side_effect=Exception("test_stop")) as m:
            result = run_search.apply_async(args=[str(self.job.id)])
            result.get(timeout=10, propagate=False)
        return m

    def test_retry_called_with_no_limit(self):
        m = self._run_task()
        m.assert_called_once()
        self.assertIsNone(m.call_args.kwargs["max_retries"])

    def test_retry_called_with_paused_countdown(self):
        m = self._run_task()
        m.assert_called_once()
        self.assertEqual(
            m.call_args.kwargs["countdown"],
            settings.HMMER.paused_retry_period_seconds,
        )

    def test_client_not_called(self):
        with patch("search.tasks.Client") as mock_client:
            with patch.object(run_search, "retry", side_effect=Exception("test_stop")):
                result = run_search.apply_async(args=[str(self.job.id)])
                result.get(timeout=10, propagate=False)
        mock_client.assert_not_called()


class RunSearchConnectionErrorTests(TransactionTestCase):
    """Worker-level tests: connection errors use bounded retries."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._worker = start_worker(celery_app, concurrency=1, perform_ping_check=False, loglevel="critical")
        cls._worker.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._worker.__exit__(None, None, None)
        super().tearDownClass()

    def setUp(self):
        self.database = Database.objects.create(
            id="pdb",
            name="PDB",
            version="1.0",
            status=Database.StatusChoices.ENABLED,
        )
        self.job = HmmerJob.add_root(database=self.database)

        self.mock_storage = MagicMock()
        self.mock_storage.save.return_value = f"{self.job.id}/hits.bin"
        self.mock_storage.path.return_value = f"/tmp/{self.job.id}/hits.bin"

        self.db_config = DatabaseSettings(host="localhost", port=51371)

    def _run_task(self, exc):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.search.side_effect = exc

        with patch.dict(settings.HMMER.databases, {"pdb": self.db_config}):
            with patch("search.tasks.storages", {"results": self.mock_storage}):
                with patch("search.tasks.Client", return_value=mock_client):
                    with patch.object(
                        run_search, "retry", side_effect=Exception("test_stop")
                    ) as m:
                        result = run_search.apply_async(args=[str(self.job.id)])
                        result.get(timeout=10, propagate=False)
        return m

    def test_connection_error_uses_bounded_max_retries(self):
        m = self._run_task(ConnectionError("refused"))
        m.assert_called_once()
        self.assertEqual(m.call_args.kwargs["max_retries"], settings.HMMER.max_retries)

    def test_connection_error_uses_standard_countdown(self):
        m = self._run_task(ConnectionError("refused"))
        m.assert_called_once()
        self.assertEqual(m.call_args.kwargs["countdown"], settings.HMMER.retry_period_seconds)

    def test_timeout_error_uses_bounded_max_retries(self):
        m = self._run_task(TimeoutError("timed out"))
        m.assert_called_once()
        self.assertEqual(m.call_args.kwargs["max_retries"], settings.HMMER.max_retries)