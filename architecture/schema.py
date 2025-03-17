from typing import List, Optional
from ninja import Schema


class Metadata(Schema):
    score_name: str
    uniq: int
    score: float
    bitscore: float
    description: str
    accession: str
    end: int
    database: str
    ali_end: int
    identifier: str
    type: str
    ali_start: int
    start: int


class Region(Schema):
    model_start: int
    model_end: int
    display: int
    color: str
    model_length: int
    text: str
    href: str
    metadata: Metadata
    ali_start: int
    end_style: str
    start_style: str
    end: int
    ali_end: int
    type: str
    clan: Optional[str]
    start: int


class MarkupMetadata(Schema):
    database: str
    evidence: str
    description: str
    start: int


class Markup(Schema):
    line_colour: str
    display: int
    residue: str
    color: str
    head_style: str
    v_align: str
    metadata: MarkupMetadata
    type: str
    start: int


class Hit(Schema):
    qstart: int
    tstart: int
    tend: int
    qend: int


class DomainGraphic(Schema):
    length: int
    hits: List[Hit]
    regions: List[Region]
    markups: List[Markup]
    title: str
    accession: str
    exact_match: int
