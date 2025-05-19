import os
from dataclasses import asdict
from itertools import groupby
from django.db import models
from django.forms.models import model_to_dict
from django.db.models.functions import MD5
from pydantic import BaseModel, Field, model_validator
from typing import List, Any
from result.models import Result


class Architecture(models.Model):
    pk = models.CompositePrimaryKey("sequence_index", "database")
    sequence_index = models.BigIntegerField()
    database = models.CharField(max_length=32)
    accessions = models.TextField()
    names = models.TextField()
    score = models.PositiveIntegerField()
    graphics = models.JSONField()

    @classmethod
    def from_results(cls, result: Result, database: str):
        sorted_hits = sorted(
            result.hits,
            key=lambda hit: (hit.metadata.architecture_checksum, hit.metadata.architecture_score, -hit.evalue),
            reverse=True,
        )

        grouped = [
            (key, list(group))
            for key, group in groupby(sorted_hits, key=lambda hit: hit.metadata.architecture_checksum)
        ]

        sequence_indexes = [int(group[0].name) for _, group in grouped]

        architecture_map = {
            architecture.sequence_index: model_to_dict(architecture)
            for architecture in Architecture.objects.filter(sequence_index__in=sequence_indexes, database=database)
        }

        architectures = []

        for checksum, group in grouped:
            representative_hit = group[0]
            architectures.append(
                {
                    "architecture": {
                        **architecture_map[int(representative_hit.name)],
                        "sequence_accession": representative_hit.metadata.accession,
                        "sequence_external_link": representative_hit.metadata.external_link,
                    },
                    "count": len(group),
                }
            )

        return sorted(architectures, key=lambda object: object["count"], reverse=True)

    @classmethod
    def from_raw_hits(cls, path: os.PathLike, db_config: Any):
        result, _ = Result.from_file(path, db_conf=db_config, with_domains=False, with_metadata=True)

        sorted_hits = sorted(
            result.hits,
            key=lambda hit: (hit.metadata.architecture_checksum, hit.metadata.architecture_score, -hit.evalue),
            reverse=True,
        )

        grouped = [
            (key, list(group))
            for key, group in groupby(sorted_hits, key=lambda hit: hit.metadata.architecture_checksum)
        ]

        sequence_indexes = [int(group[0].name) for _, group in grouped]

        architecture_map = {
            architecture.sequence_index: model_to_dict(architecture)
            for architecture in Architecture.objects.filter(
                sequence_index__in=sequence_indexes, database=db_config.architecture_database
            )
        }

        architectures = []

        for _, group in grouped:
            representative_hit = group[0]
            hit_index = representative_hit.index
            result_with_metadata, _ = Result.from_file(
                path, db_conf=db_config, start=hit_index, end=hit_index + 1, with_metadata=True, with_domains=True
            )
            hit_with_metadata = result_with_metadata.hits[0]

            architecture = architecture_map[int(representative_hit.name)]

            architecture["graphics"]["hits"] = [
                {
                    "tstart": domain.alignment_display.sqfrom,
                    "tend": domain.alignment_display.sqto,
                    "qstart": domain.alignment_display.hmmfrom,
                    "qend": domain.alignment_display.hmmto,
                }
                for domain in hit_with_metadata.domains
                if domain.is_included
            ]

            architectures.append(
                {
                    "architecture": {
                        **architecture,
                        "sequence_accession": hit_with_metadata.metadata.accession,
                        "sequence_external_link": hit_with_metadata.metadata.external_link,
                    },
                    "count": len(group),
                }
            )

        return sorted(architectures, key=lambda object: object["count"], reverse=True)

    class Meta:
        indexes = [
            models.Index(fields=["sequence_index", "database"]),
            models.Index(MD5("accessions"), name="idx_accessions_md5"),
        ]


class Region(BaseModel):
    color: str = Field(default="")
    end_style: str = Field(default="curved")
    start_style: str = Field(default="curved")
    display: bool = Field(default=True)
    href: str = Field(default="")
    clan: str = Field(default="")
    metadata: dict = Field(default={})
    type: str = Field(default="pfam")
    text: str
    model_length: int
    model_start: int
    model_end: int
    start: int
    end: int
    ali_start: int
    ali_end: int

    @model_validator(mode="after")
    def set_end_styles(self):
        if "type" in self.metadata and self.metadata["type"] in {"Repeat", "Motif"}:
            self.start_style = "straight"
            self.end_style = "straight"
        else:
            if self.model_start != 1:
                self.start_style = "jagged"

            if "model_length" not in self.metadata or self.metadata["model_length"] != self.model_end:
                self.end_style = "jagged"

        return self


class Markup(BaseModel):
    line_color: str = Field(default="#333333")
    color: str = Field(default="#e469fe")
    display: bool = Field(default=True)
    residue: bool = Field(default="X")
    headstyle: str = Field(default="diamond")
    v_align: str = Field(default="top")
    type: str = Field(default="Predicted active site")
    metadata: dict = Field(default={})
    start: int


class Annotation(BaseModel):
    length: int
    regions: List[Region]

    @classmethod
    def from_results(cls, result: Result) -> "Annotation":
        regions = []
        markups = []

        for hit in result.hits:
            for domain in filter(lambda d: d.is_included and d.display, hit.domains):
                if domain.predicted_active_sites is not None:
                    for evidence, positions in domain.predicted_active_sites:
                        for position in positions:
                            markups.append(
                                {
                                    "metadata": {
                                        "database": "pfam",
                                        "evidence": evidence,
                                        "description": "Pfam predicted active site",
                                    },
                                    "start": position,
                                }
                            )

                if domain.segments is None:
                    domain.segments = [(domain.ienv, domain.jenv)]

                for i, (start, end) in enumerate(domain.segments):
                    regions.append(
                        {
                            **asdict(domain),
                            "color": hit.metadata.color,
                            "model_length": domain.alignment_display.m,
                            "model_start": domain.alignment_display.hmmfrom,
                            "model_end": domain.alignment_display.hmmto,
                            "start": start,
                            "end": end,
                            "ali_start": domain.iali if i == 0 and domain.iali > start else start,
                            "ali_end": domain.jali if i == len(domain.segments) - 1 and end > domain.jali else end,
                            "text": hit.metadata.identifier,
                            "metadata": {
                                "model_length": hit.metadata.model_length,
                                "type": hit.metadata.type,
                                "_uniq": domain.uniq,
                                "score_name": "e-value",
                                "score": domain.cevalue,
                                "bitscore": domain.bitscore,
                                "description": hit.metadata.description,
                                "accession": hit.metadata.accession,
                                "end": domain.jenv,
                                "database": "pfam",
                                "ali_end": domain.jali,
                                "identifier": hit.metadata.identifier,
                                "ali_start": domain.iali,
                                "start": domain.ienv,
                            },
                        }
                    )
        sequence_length = result.hits[0].domains[0].alignment_display.l if result.stats.nhits > 0 else 0

        return cls(length=sequence_length, regions=regions)
