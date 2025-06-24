import os
import concurrent.futures
import gzip
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TextIO, Union, List
from django.db import models
from django.conf import settings
from django.core.files.storage import storages
from django.template import loader
from django_celery_results.models import TaskResult
from pyhmmer.easel import SSIReader, TextSequence, TextSequenceBlock
from result.models import Result
from search.models import HmmerJob
from utils.functions import msa_from_hmmpgmd


formats = {
    "text": {
        "name": "Text",
        "description": "A plain text file containing the hit alignments and scores.",
        "extension": "txt",
    },
    "tsv": {
        "name": "Tab Delimited",
        "description": "A tab delimited text file containing the hit information. No alignments.",
        "extension": "tsv",
    },
    "fasta": {
        "name": "FASTA",
        "description": "Download the significant hits from your search as a gzipped FASTA file.",
        "extension": "fa",
        "gzip": True,
    },
    "fullfasta": {
        "name": "Full length FASTA",
        "description": "A gzipped file containing the full length sequences for significant search hits.",
        "extension": "fa",
        "gzip": True,
    },
    "afa": {
        "name": "Aligned FASTA",
        "description": "A gzipped file containing aligned significant search hits in FASTA format.",
        "extension": "afa",
        "gzip": True,
    },
    "stockholm": {
        "name": "STOCKHOLM",
        "description": "Download an alignment of significant hits as a gzipped STOCKHOLM file.",
        "extension": "sto",
        "gzip": True,
    },
    "clustal": {
        "name": "ClustalW",
        "description": "Download an alignment of significant hits as a gzipped ClustalW file.",
        "extension": "clu",
        "gzip": True,
    },
    "psiblast": {
        "name": "PSI-BLAST",
        "description": "Download an alignment of significant hits as a gzipped psiblast file.",
        "extension": "psi",
        "gzip": True,
    },
    "phylip": {
        "name": "PHYLIP",
        "description": "Download an alignment of significant hits as a gzipped phylip file.",
        "extension": "phy",
        "gzip": True,
    },
}

allowed_formats = {
    "phmmer": ["text", "tsv", "fasta", "fullfasta", "afa", "stockholm", "clustal", "psiblast", "phylip"],
    "hmmsearch": ["text", "tsv", "fasta", "fullfasta", "afa", "stockholm", "clustal", "psiblast", "phylip"],
    "hmmscan": ["text", "tsv"],
    "jackhmmer": ["text", "tsv", "fasta", "fullfasta", "afa", "stockholm", "clustal", "psiblast", "phylip"],
}


class FileJob(models.Model):
    job = models.ForeignKey(HmmerJob, related_name="+", on_delete=models.CASCADE)
    format = models.CharField(max_length=32)
    task = models.OneToOneField(TaskResult, related_name="+", null=True, blank=True, on_delete=models.CASCADE)
    filters = models.JSONField(default=dict)
    file = models.FileField(storage=storages["downloads"], blank=True, null=True)


class FileBuildStrategy(ABC):
    def __init__(self, file_job: FileJob):
        self.file_job = file_job
        self.format = file_job.format

        try:
            db_config = settings.HMMER.databases[file_job.job.database.id]
        except KeyError:
            raise ValueError(f"Database {file_job.job.database.id} not found in settings")

        result, _ = Result.from_file(
            file_job.job.result_file,
            db_conf=db_config,
            with_domains=True,
            algo=file_job.job.algo,
            id=file_job.job.id,
            taxonomy_ids=file_job.filters["taxonomy_ids"],
            architecture=file_job.filters["architecture"],
        )
        self.db_conf = db_config
        self.result = result

    @abstractmethod
    def build(self, path: os.PathLike):
        pass

    @contextmanager
    def open(self, path: os.PathLike, mode: str):
        if "gzip" in formats[self.format] and formats[self.format]["gzip"]:
            fh = gzip.open(path, mode)
        else:
            fh = open(path, mode)

        try:
            yield fh
        finally:
            fh.close()


class TemplateBuildStrategy(FileBuildStrategy):
    def build(self, path: os.PathLike):
        header_template = loader.get_template(f"{self.format}/header/{self.result.stats.algo}.txt")
        body_template = loader.get_template(f"{self.format}/body/{self.result.stats.algo}.txt")

        with self.open(path, mode="wt") as fh:
            header = header_template.render({"algo": self.result.stats.algo, "id": self.result.stats.id})
            body = body_template.render({"hits": [hit for hit in self.result.hits if hit.is_included]})
            fh.write(header)
            fh.write(body)


class FastaBuildStrategy(FileBuildStrategy):
    def __init__(self, file_job: FileJob):
        super().__init__(file_job)
        self.full_length = self.format.startswith("full")

    def build(self, path: os.PathLike):
        with self.open(path, mode="wb") as fh:
            for sequence in self.fetch_sequences():
                sequence.write(fh)

    def fetch_sequence(self, fh: TextIO, ssi_reader: SSIReader, key: Union[str, int]):
        entry = ssi_reader.find_name(str(key).encode())
        fh.seek(entry.data_offset)
        return fh.read(entry.record_length)

    def fetch_sequence_chunk(self, file: os.PathLike, keys: Union[List[str], List[int]]):
        with SSIReader(f"{file}.ssi") as ssi_reader, open(file, "rt") as fh:
            return {key: self.fetch_sequence(fh, ssi_reader, key) for key in keys}

    def fetch_sequences(self):
        max_workers = 4
        chunk_size = 1000
        keys = [int(hit.name) for hit in self.result.hits if hit.is_included]
        chunks = [keys[i: i + chunk_size] for i in range(0, len(keys), chunk_size)]

        all_sequences: dict[str | int, str] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.fetch_sequence_chunk, self.db_conf.db_file_location, chunk) for chunk in chunks
            ]

            for future in concurrent.futures.as_completed(futures):
                all_sequences.update(future.result())

        if self.full_length:
            return TextSequenceBlock(
                [
                    TextSequence(name=hit.metadata.identifier.encode(), sequence=all_sequences[int(hit.name)])
                    for hit in self.result.hits
                    if hit.is_included
                ]
            )
        else:
            return TextSequenceBlock(
                [
                    TextSequence(
                        name=f"{hit.metadata.identifier}/{domain.ienv}-{domain.jenv}".encode(),
                        sequence=all_sequences[int(hit.name)][domain.ienv - 1: domain.jenv],
                    )
                    for hit in self.result.hits
                    for domain in hit.domains
                    if hit.is_included
                ]
            )


class MSABuildStrategy(FileBuildStrategy):
    def build(self, path: os.PathLike):
        if self.file_job.filters["taxonomy_ids"] or self.file_job.filters["architecture"]:
            result, _ = Result.from_file(
                self.file_job.job.result_file,
                with_domains=False,
                with_metadata=True,
                db_conf=self.db_conf,
                taxonomy_ids=self.file_job.filters["taxonomy_ids"],
                architecture=self.file_job.filters["architecture"],
            )

            include = [int(hit.name) for hit in result.hits if hit.is_included]
        else:
            include = []

        msa = msa_from_hmmpgmd(
            self.file_job.job.result_file,
            self.file_job.job.input_hmm,
            self.format,
            self.db_conf,
            include,
        )

        with self.open(path, mode="wt") as fh:
            fh.write(msa)
