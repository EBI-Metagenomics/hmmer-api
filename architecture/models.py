from dataclasses import asdict
from django.db import models
from django.contrib.postgres.aggregates import ArrayAgg
from django.contrib.postgres.indexes import HashIndex
from pydantic import BaseModel, Field, model_validator
from typing import List
from result.models import Result


class Architecture(models.Model):
    pk = models.CompositePrimaryKey("sequence_index", "database")
    sequence_index = models.BigIntegerField()
    database = models.CharField(max_length=32)
    accessions = models.TextField()
    names = models.TextField()
    score = models.PositiveIntegerField()
    graphics = models.TextField()

    @classmethod
    def from_results(cls, result: Result, database: str):
        sequence_indexes = [int(hit.name) for hit in result.hits]
        sequence_accessions = {int(hit.name): hit.metadata.accession for hit in result.hits}
        sequence_external_links = {int(hit.name): hit.metadata.external_link for hit in result.hits}
        sequence_evalues = {int(hit.name): hit.evalue for hit in result.hits}

        grouped = (
            Architecture.objects.filter(sequence_index__in=sequence_indexes)
            .filter(database=database)
            .order_by("accessions")
            .values("accessions", "names")
            .annotate(
                sequence_index_list=ArrayAgg("sequence_index"),
                score_list=ArrayAgg("score"),
                graphics_list=ArrayAgg("graphics"),
            )
        )

        return [
            sorted(
                [
                    {
                        "sequence_index": sequence_index,
                        "sequence_accession": sequence_accessions[sequence_index],
                        "sequence_external_link": sequence_external_links[sequence_index],
                        "accessions": group["accessions"],
                        "names": group["names"],
                        "score": score,
                        "graphics": graphics,
                    }
                    for sequence_index, score, graphics in zip(
                        group["sequence_index_list"], group["score_list"], group["graphics_list"]
                    )
                ],
                key=lambda architecture: (architecture["score"], -sequence_evalues[architecture["sequence_index"]]),
                reverse=True,
            )
            for group in grouped
        ]

    class Meta:
        indexes = [HashIndex(fields=["accessions"])]


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
