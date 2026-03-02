from typing import Any

from django.conf import settings
from django.db import connections
from django.test.runner import DiscoverRunner
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


class TestRunner(DiscoverRunner):
    def setup_test_environment(self, **kwargs: Any) -> None:
        # Start PostgreSQL
        self.postgres = PostgresContainer("postgres:16")
        self.postgres.start()

        # Start Redis
        self.redis = RedisContainer("redis:7-alpine")
        self.redis.start()

        # Update database settings
        settings.DATABASES["default"] = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": self.postgres.dbname,
            "USER": self.postgres.username,
            "PASSWORD": self.postgres.password,
            "HOST": self.postgres.get_container_host_ip(),
            "PORT": self.postgres.get_exposed_port(5432),
        }

        if "settings" in connections.__dict__:
            del connections.__dict__["settings"]

        # Reconfigure connections with updated settings
        connections._settings = connections.configure_settings(settings.DATABASES)  # type: ignore[attr-defined]

        # Close all existing connections
        connections.close_all()

        # Explicitly recreate connections with new settings
        for alias in settings.DATABASES:
            connections[alias] = connections.create_connection(alias)

        # Update cache/celery settings
        redis_url = f"redis://{self.redis.get_container_host_ip()}:{self.redis.get_exposed_port(6379)}"
        settings.CACHES = {
            "default": {
                "BACKEND": "django.core.cache.backends.redis.RedisCache",
                "LOCATION": redis_url,
            }
        }
        settings.CELERY_BROKER_URL = redis_url

        return super().setup_test_environment(**kwargs)

    def teardown_test_environment(self, **kwargs: Any) -> None:
        super().teardown_test_environment(**kwargs)

        self.postgres.stop()
        self.redis.stop()
