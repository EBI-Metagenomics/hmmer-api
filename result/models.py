import json
import re
from functools import cached_property
from typing import List, Optional, Any, Union, Tuple
from django.conf import settings
from pydantic import (
    BaseModel,
    ConfigDict,
    model_validator,
    ValidationInfo,
    Field,
    field_validator,
    AliasPath,
    computed_field,
)
from pyhmmer.plan7 import TopHits
import logging

logger = logging.getLogger(__name__)


class Structure(BaseModel):
    id: str
    external_link: str


class Metadata(BaseModel):
    structures: list[Structure] = Field([], alias="s")
    taxonomy_id: int = Field(alias="t")
    architecture_checksum: int = Field(alias="ai")
    architecture_score: int = Field(alias="as")
    architecture: str = Field(alias="a")
    accession: str = Field(alias=AliasPath("m", "a"))
    identifier: str = Field(alias=AliasPath("m", "i"))
    description: str = Field(alias=AliasPath("m", "d"))
    uniprot_accession: Optional[str] = Field(alias=AliasPath("m", "u"))
    uniprot_identifier: Optional[str] = Field(alias=AliasPath("m", "v"))
    kingdom: Optional[str] = Field(alias=AliasPath("m", "k"))
    phylum: Optional[str] = Field(alias=AliasPath("m", "p"))
    species: Optional[str] = Field(alias=AliasPath("m", "s"))

    external_link: str = Field(alias=AliasPath("m", "a"))
    taxonomy_link: str = Field(alias="t")

    @field_validator("external_link", mode="before", check_fields=False)
    @classmethod
    def set_external_link(cls, data: Any, info: ValidationInfo):
        external_link_template = info.context["db_conf"].external_link_template
        return external_link_template.format(data)

    @field_validator("taxonomy_link", mode="before", check_fields=False)
    @classmethod
    def set_taxonomy_link(cls, data: Any, info: ValidationInfo):
        if info.context["db_conf"].taxonomy_link_template is None:
            return ""

        external_link_template = info.context["db_conf"].taxonomy_link_template
        return external_link_template.format(data)

    @field_validator("structures", mode="before", check_fields=False)
    @classmethod
    def set_structures(cls, data, info: ValidationInfo):
        if info.context["db_conf"].structure_link_template is None:
            return ""

        external_link_template = info.context["db_conf"].structure_link_template
        pattern = r"AF-(P\d+)-F\d+"
        uniprot_ids = [re.search(pattern, id).group(1) for id in data if re.search(pattern, id)]

        return [
            Structure(id=id, external_link=external_link_template.format(uniprot_id))
            for id, uniprot_id in zip(data, uniprot_ids)
        ]

    @model_validator(mode="before")
    @classmethod
    def convert_to_dict(cls, data: Any, info: ValidationInfo):
        db_id = info.context["db_conf"].db
        entries = filter(lambda x: x["d"] == db_id, data["d"])
        data["m"] = next(entries)["m"]
        return data


class Alignment(BaseModel):
    hmm_accession: str
    hmm_from: int
    hmm_length: int
    hmm_name: str
    hmm_sequence: str
    hmm_to: int
    identity_sequence: str
    target_from: int
    target_length: int
    target_name: str
    target_sequence: str
    target_to: int

    @cached_property
    def identity(self) -> Tuple[float, int]:
        return self._pair_identity(self.hmm_sequence, self.identity_sequence, self.target_sequence)

    @cached_property
    def similarity(self) -> Tuple[float, int]:
        return self._pair_similarity(self.hmm_sequence, self.identity_sequence, self.target_sequence)

    @computed_field
    @property
    def identity_score(self) -> float:
        return self.identity[0]

    @computed_field
    @property
    def similarity_score(self) -> float:
        return self.similarity[0]

    @computed_field
    @property
    def identity_count(self) -> int:
        return self.identity[1]

    @computed_field
    @property
    def similarity_count(self) -> int:
        return self.similarity[1]

    def _pair_identity(self, seq1, match, seq2):
        if len(seq1) != len(seq2):
            print("Unaligned sequences")
            return None, None

        match = "".join(filter(str.isalpha, match))
        seq1 = "".join(filter(str.isalpha, seq1))
        seq2 = "".join(filter(str.isalpha, seq2))

        pairScore, count = self._pair(seq1, match, seq2)
        return pairScore, count

    def _pair(self, seq1, match, seq2):
        x = len(seq1)
        y = len(seq2)
        noId = len(match)
        min_len = min(x, y)

        if min_len and noId:
            return noId / min_len, noId
        else:
            return 0, 0

    def _pair_similarity(self, seq1, match, seq2):
        if len(seq1) != len(seq2):
            print("Unaligned sequences")
            return None, None

        match = "".join(filter(str.isalpha, match))
        seq1 = "".join(filter(str.isalpha, seq1))
        seq2 = "".join(filter(str.isalpha, seq2))

        pairScore, count = self._pair(seq1, match, seq2)
        return pairScore, count

    def pairIdAndSim(self, seq1, match, seq2):
        idScore, idCount = self.pairID(seq1, match, seq2)
        simScore, simCount = self.pairSim(seq1, match, seq2)
        return idScore, idCount, simScore, simCount

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Domain(BaseModel):
    alignment: Alignment
    bias: float
    c_evalue: float
    correction: float
    env_from: int
    env_to: int
    envelope_score: float
    i_evalue: float
    included: bool
    pvalue: float
    reported: bool
    score: float

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Domains(BaseModel):
    included: List[Domain]
    reported: List[Domain]

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Hit(BaseModel):
    bias: float
    domains: Domains
    dropped: bool
    duplicate: bool
    evalue: float
    included: bool
    length: int
    name: str
    new: bool
    pre_score: float
    pvalue: float
    reported: bool
    score: float
    sum_score: float
    metadata: Metadata = Field(alias="description")
    index: Optional[int] = Field(default=-1)

    @field_validator("metadata", mode="before")
    @classmethod
    def transform_metadata(cls, data: Any) -> Any:
        if isinstance(data, bytes):
            return json.loads(data.decode())
        if isinstance(data, str):
            return json.loads(data)
        return data

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Stats(BaseModel):
    E: float
    T: Optional[float]
    Z: float
    bit_cutoffs: Optional[str]
    domE: float
    domT: Optional[float]
    domZ: float
    incE: float
    incT: Optional[float]
    incdomE: float
    incdomT: Optional[float]
    searched_models: int
    searched_nodes: int
    searched_residues: int
    searched_sequences: int
    included_hits: int = Field(alias="included")
    reported_hits: int = Field(alias="reported")

    @field_validator("included_hits", "reported_hits", mode="before")
    @classmethod
    def transform_stats(cls, data: Any) -> Any:
        return len(data)

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Result(BaseModel):
    hits: List[Hit]
    stats: Stats

    @field_validator("hits", mode="after")
    @classmethod
    def assign_index(cls, data: Any):
        for i, hit in enumerate(data):
            hit.index = i
        return data

    @classmethod
    def from_top_hits(cls, top_hits: TopHits, job_params: dict, index: Optional[Union[int, slice]] = None):
        db_config = None

        if "seqdb" in job_params:
            [db_config] = [config for config in settings.HMMER.databases if config.name == job_params["seqdb"]]

            if db_config is None:
                raise ValueError(f"No config found for {job_params['seqdb']}")

        if index is None:
            return cls.model_validate({"hits": list(top_hits), "stats": top_hits}, context={"db_conf": db_config})
        elif isinstance(index, int):
            return cls.model_validate({"hits": [top_hits[index]], "stats": top_hits}, context={"db_conf": db_config})
        elif isinstance(index, slice):
            start = index.start or 0
            stop = index.stop or len(top_hits)
            step = index.step or 1

            try:
                hits = [top_hits[i] for i in range(start, stop, step)]
            except IndexError:
                hits = []
            return cls.model_validate({"hits": hits, "stats": top_hits}, context={"db_conf": db_config})

    model_config = ConfigDict(from_attributes=True, extra="ignore")
