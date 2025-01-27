from pathlib import Path
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class DjangoSettings(BaseSettings):
    database_url: str = f"sqlite:///{BASE_DIR / 'db.sqlite3'}"
    cache_url: str = "dummy://"

    model_config = SettingsConfigDict(env_prefix="DJANGO_")


class DatabaseSettings(BaseSettings):
    name: str
    host: str = "localhost"
    port: int = 51371
    db: int = 1
    external_link_template: str = "https://www.uniprot.org/uniprotkb/{}/entry"
    taxonomy_link_template: str = "https://www.uniprot.org/taxonomy/{}"
    structure_link_template: str = "https://alphafold.ebi.ac.uk/entry/{}"


class HmmerSettings(BaseSettings):
    allowed_algorithms: List[str] = ["phmmer"]
    databases: List[DatabaseSettings] = []
    results_storage_location: str = f"{BASE_DIR / 'media' / 'results'}"

    model_config = SettingsConfigDict(env_prefix="HMMER_")


class CelerySettings(BaseSettings):
    broker_url: str = ""

    model_config = SettingsConfigDict(env_prefix="CELERY_")
