from hmmerapi.config import DjangoSettings


def test_build_databases_parses_database_url():
    from hmmerapi.settings import build_databases

    config = DjangoSettings.model_validate(
        {
            "database_url": "postgres://db_user:db_password@db.example.org:5433/hmmer_prod",
        }
    )

    databases = build_databases(config)

    assert databases["default"]["ENGINE"] == "django.db.backends.postgresql"
    assert databases["default"]["NAME"] == "hmmer_prod"
    assert databases["default"]["USER"] == "db_user"
    assert databases["default"]["PASSWORD"] == "db_password"
    assert databases["default"]["HOST"] == "db.example.org"
    assert databases["default"]["PORT"] == 5433
    assert databases["default"]["CONN_MAX_AGE"] == 600
    assert databases["default"]["CONN_HEALTH_CHECKS"] is True


def test_build_databases_uses_discrete_database_fields_without_url():
    from hmmerapi.settings import build_databases

    config = DjangoSettings.model_validate(
        {
            "database_name": "hmmer_local",
            "database_user": "local_user",
            "database_password": "local_password",
            "database_host": "localhost",
            "database_port": 15432,
        }
    )

    databases = build_databases(config)

    assert databases == {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "hmmer_local",
            "USER": "local_user",
            "PASSWORD": "local_password",
            "HOST": "localhost",
            "PORT": 15432,
        }
    }


def test_default_cors_settings_do_not_allow_all_origins_with_credentials():
    from hmmerapi.settings import CORS_ALLOW_ALL_ORIGINS, CORS_ALLOW_CREDENTIALS

    assert CORS_ALLOW_ALL_ORIGINS is False
    assert CORS_ALLOW_CREDENTIALS is False
