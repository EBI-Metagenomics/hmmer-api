from pathlib import Path
from typing import List, Dict, Optional, Any
from pydantic import ImportString
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class DjangoSettings(BaseSettings):
    secret_key: Optional[str] = "django-insecure-+es_-5afm=y4du+nt2ypvwiaxwo6iuf8!^qjq*jbkf^(46^&3r"
    csrf_trusted_origins: List[str] = []
    base_url: str = ""  # for EBI use 'Tools/hmmer/' (no leading slash)
    build_https_download_urls: bool = False

    database_url: Optional[str] = None
    cache_url: str = "dummy://"

    database_name: str = "hmmer"
    database_user: str = "hmmer"
    database_password: str = "dummypassword"
    database_host: str = "localhost"
    database_port: int = 5432

    debug: Optional[bool] = False

    model_config = SettingsConfigDict(env_prefix="DJANGO_")


class DatabaseSettings(BaseSettings):
    name: str = ""
    host: str = "localhost"
    port: int = 51371
    db: Optional[int] = 1
    metadata_model_class: Optional[ImportString] = "result.models:Metadata"
    db_file_location: Optional[str] = "db.hmmpgmd"
    external_link_template: Optional[str] = "https://www.uniprot.org/uniprotkb/{}/entry"
    taxonomy_link_template: Optional[str] = "https://www.uniprot.org/taxonomy/{}"
    structure_link_template: Optional[str] = "https://alphafold.ebi.ac.uk/entry/{}"
    architecture_database: Optional[str] = ""


class HmmerSettings(BaseSettings):
    allowed_algorithms: List[str] = ["phmmer"]
    annotation_db: Optional[str] = "pfam"
    databases: Dict[str, DatabaseSettings] = {}
    results_storage_location: str = f"{BASE_DIR / 'results'}"
    downloads_storage_location: str = f"{BASE_DIR / 'downloads'}"

    jackhmmer_max_iterations: int = 9
    jackhmmer_max_batch_iterations: int = 5

    model_config = SettingsConfigDict(env_prefix="HMMER_")


class CelerySettings(BaseSettings):
    broker_url: str = ""
    task_routes: Optional[Dict[str, Any]] = None
    model_config = SettingsConfigDict(env_prefix="CELERY_")
