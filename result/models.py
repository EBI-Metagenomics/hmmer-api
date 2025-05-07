import io
import os
import re
import math

from construct import (
    Float64b,
    Float32b,
    Int8ub,
    Int64ub,
    Int32ub,
    Pointer,
    Struct,
    Array,
    Computed,
    this,
    FlagsEnum,
    CString,
    If,
    Tell,
)
from construct_typed import DataclassMixin, DataclassStruct, csfield, FlagsEnumBase, EnumBase
from pydantic import BaseModel, Field, model_serializer, AliasPath, field_validator, ValidationInfo, model_validator
from pydantic.dataclasses import dataclass
from dataclasses import asdict
from typing import List, Optional, TypeVar, Type, Any, Dict, Tuple

from hmmerapi.config import DatabaseSettings


T = TypeVar("T")

# Constants


class HmmpgmdStatus(EnumBase):
    OK = 0  # no error/success
    FAIL = 1  # failure
    EOL = 2  # end-of-line (often normal)
    EOF = 3  # end-of-file (often normal)
    EOD = 4  # end-of-data (often normal)
    EMEM = 5  # malloc or realloc failed
    ENOTFOUND = 6  # file or key not found
    EFORMAT = 7  # file format not correct
    EAMBIGUOUS = 8  # an ambiguity of some sort
    EDIVZERO = 9  # attempted div by zero
    EINCOMPAT = 10  # incompatible parameters
    EINVAL = 11  # invalid argument/parameter
    ESYS = 12  # generic system call failure
    ECORRUPT = 13  # unexpected data corruption
    EINCONCEIVABLE = 14  # "can't happen" error
    ESYNTAX = 15  # invalid user input syntax
    ERANGE = 16  # value out of allowed range
    EDUP = 17  # saw a duplicate of something
    ENOHALT = 18  # a failure to converge
    ENORESULT = 19  # no result was obtained
    ENODATA = 20  # no data provided, file empty
    ETYPE = 21  # invalid type of argument
    EOVERWRITE = 22  # attempted to overwrite data
    ENOSPACE = 23  # ran out of some resource
    EUNIMPLEMENTED = 24  # feature is unimplemented
    ENOFORMAT = 25  # couldn't guess file format
    ENOALPHABET = 26  # couldn't guess seq alphabet
    EWRITE = 27  # write failed (fprintf, etc)
    EINACCURATE = 28  # return val may be inaccurate
    EUNSUPPORTEDISA = 29  # function requires an unsupported


# class HmmpgmdResultType(EnumBase):
#     SEQUENCE = 101
#     HMM = 102


class ZSetByEnum(EnumBase):
    ZSETBY_NTARGETS = 0
    ZSETBY_OPTION = 1
    ZSETBY_FILEINFO = 2


class P7HitFlags(FlagsEnumBase):
    IS_INCLUDED = 1 << 0
    IS_REPORTED = 1 << 1
    IS_NEW = 1 << 2
    IS_DROPPED = 1 << 3
    IS_DUPLICATE = 1 << 4


class P7HitStringPresenceFlags(FlagsEnumBase):
    ACC_PRESENT = 1 << 0
    DESC_PRESENT = 1 << 1


class P7AliStringPresenceFlags(FlagsEnumBase):
    RFLINE_PRESENT = 1 << 0
    MMLINE_PRESENT = 1 << 1
    CSLINE_PRESENT = 1 << 2
    PPLINE_PRESENT = 1 << 3
    ASEQ_PRESENT = 1 << 4
    NTSEQ_PRESENT = 1 << 5


class Structure(BaseModel):
    id: str
    external_link: str


class PfamMetadata(BaseModel):
    accession: str = Field(alias="a")
    identifier: str = Field(alias="i")
    description: str = Field(alias="d")
    clan: str = Field(alias="c")
    type: str = Field(alias="t")
    seq_ga: float = Field(alias="sg")
    dom_ga: float = Field(alias="dg")
    nested: Optional[List[str]] = Field(alias="n")
    model_length: int = Field(alias="l")
    color: str = Field(alias="cl")
    active_sites: Optional[List[Tuple[str, List[str]]]] = Field(alias="as")

    external_link: str = Field(alias="a")
    clan_link: str = Field(alias="c")

    @field_validator("external_link", mode="before", check_fields=False)
    @classmethod
    def set_external_link(cls, data: Any, info: ValidationInfo):
        return f"https://www.ebi.ac.uk/interpro/entry/pfam/{data}"

    @field_validator("clan_link", mode="before", check_fields=False)
    @classmethod
    def set_clan_link(cls, data: Any, info: ValidationInfo):
        return f"https://www.ebi.ac.uk/interpro/set/pfam/{data}"


class Metadata(BaseModel):
    structures: list[Structure] = Field([], alias="s")
    taxonomy_id: Optional[int] = Field(alias="t")
    lineage: List[Optional[int]] = Field(alias="l")
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
        db_name = info.context["db_conf"].name

        if db_name == "pdb":
            return external_link_template.format(re.sub(r"_.*$", "", data))

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
        db_index = info.context["db_conf"].db
        entries = filter(lambda x: x["d"] == db_index, data["d"])
        data["m"] = next(entries)["m"]
        return data


class HmmpgmdModel(DataclassMixin):
    @classmethod
    def from_bytes(cls: Type[T], data: bytes, **kwargs) -> T:
        format = DataclassStruct(cls)

        return format.parse(data, **kwargs)

    @classmethod
    def from_binary(cls: Type[T], stream: io.IOBase, offset=0, **kwargs) -> T:
        if offset > 0 and stream.seekable():
            stream.seek(offset)

        format = DataclassStruct(cls)

        return format.parse_stream(stream, **kwargs)

    @classmethod
    def from_file(cls: Type[T], file: os.PathLike, offset=0, **kwargs) -> T:
        format = DataclassStruct(cls)

        with open(file, mode="rb") as fh:
            if offset > 0 and fh.seekable():
                fh.seek(offset)

            return format.parse_stream(fh, **kwargs)

    @classmethod
    def size(cls: Type[T]):
        return DataclassStruct(cls).sizeof()


@dataclass
class HmmdSearchStatus(HmmpgmdModel):
    status: HmmpgmdStatus = csfield(Int32ub)
    """Contains an Easel result code"""
    # type: HmmpgmdResultType = csfield(Enum(Int32ub, HmmpgmdResultType))
    # """SEQUENCE or HMM"""
    message_size: int = csfield(Int64ub)
    """Length (in bytes) of the remaining data that will be sent to the client"""


@dataclass
class HmmdSearchStats(HmmpgmdModel):
    id: str = csfield(Computed(lambda ctx: ctx._params.get("id", "")))
    """UUID of the search/scan job"""
    algo: str = csfield(Computed(lambda ctx: ctx._params.get("algo", "unknown")))
    """Algorith by which the search was performed"""
    database: str = csfield(Computed(lambda ctx: ctx._params.get("database", "unknown")))
    """Target database"""
    elapsed: float = csfield(Float64b)
    """Elapsed time, seconds"""
    user: float = csfield(Float64b)
    """CPU time, seconds"""
    sys: float = csfield(Float64b)
    """System time, seconds"""
    Z: float = csfield(Float64b)
    """Effective number of targs searched (per-target E-val)"""
    domZ: float = csfield(Float64b)
    """Effective number of signific targs (per-domain E-val)"""
    Z_setby: int = csfield(Int8ub)
    """How Z was set"""
    domZ_setby: int = csfield(Int8ub)
    """How domZ was set"""
    nmodels: int = csfield(Int64ub)
    """Number of HMMs searched"""
    # nnodes: int = csfield(Int64ub)
    # """Number of HMM nodes searched"""
    nseqs: int = csfield(Int64ub)
    """Number of sequences searched"""
    # nres: int = csfield(Int64ub)
    # """Number of residues searched"""
    n_past_msv: int = csfield(Int64ub)
    """Number of comparisons that pass MSVFilter()"""
    n_past_bias: int = csfield(Int64ub)
    """Number of comparisons that pass bias filter"""
    n_past_vit: int = csfield(Int64ub)
    """Number of comparisons that pass ViterbiFilter()"""
    n_past_fwd: int = csfield(Int64ub)
    """Number of comparisons that pass ForwardFilter()"""
    nhits: int = csfield(Int64ub)
    """Number of hits in list now"""
    nreported: int = csfield(Int64ub)
    """Number of hits that are reportable"""
    nincluded: int = csfield(Int64ub)
    """Number of hits that are includable"""
    hit_offsets: Optional[List[int]] = csfield(Array(this.nhits, Int64ub))
    """An array of nhits values that define the offsets"""
    size: int = csfield(Tell)
    """length (in bytes) of the serialized HmmdSearchStats object"""

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        fields_to_exclude = ["size", "hit_offsets"]
        old_dict = asdict(self)

        return {key: old_dict[key] for key in old_dict if key not in fields_to_exclude}


@dataclass
class P7AlignmentDisplay(HmmpgmdModel):
    size: int = csfield(Int32ub)
    """length (in bytes) of the serialized P7_ALIDISPLAY object"""
    n: int = csfield(Int32ub)
    """Length of strings"""
    hmmfrom: int = csfield(Int32ub)
    """Start position on HMM (1..M, or -1)"""
    hmmto: int = csfield(Int32ub)
    """End position on HMM (1..M, or -1)"""
    m: int = csfield(Int32ub)
    """Length of model"""
    sqfrom: int = csfield(Int64ub)
    """Start position on sequence (1..L)"""
    sqto: int = csfield(Int64ub)
    """End position on sequence (1..L)"""
    l: int = csfield(Int64ub)
    """Length of sequence"""
    string_presence_flags: Any = csfield(FlagsEnum(Int8ub, P7AliStringPresenceFlags))
    """String presence flags"""
    rfline: Optional[str] = csfield(If(this.string_presence_flags.RFLINE_PRESENT, CString("utf8")))
    """Reference coord info"""
    mmline: Optional[str] = csfield(If(this.string_presence_flags.MMLINE_PRESENT, CString("utf8")))
    """Modelmask coord info"""
    csline: Optional[str] = csfield(If(this.string_presence_flags.CSLINE_PRESENT, CString("utf8")))
    """Consensus structure info"""
    model: str = csfield(CString("utf8"))
    """Aligned query consensus sequence"""
    mline: str = csfield(CString("utf8"))
    """"identities", conservation +'s, etc."""
    aseq: Optional[str] = csfield(If(this.string_presence_flags.ASEQ_PRESENT, CString("utf8")))
    """Aligned target sequence"""
    ntseq: Optional[str] = csfield(If(this.string_presence_flags.NTSEQ_PRESENT, CString("utf8")))
    """Nucleotide target sequence if nhmmscan"""
    ppline: Optional[str] = csfield(If(this.string_presence_flags.PPLINE_PRESENT, CString("utf8")))
    """Posterior prob annotation"""
    hmmname: str = csfield(CString("utf8"))
    """Name of HMM"""
    hmmacc: str = csfield(CString("utf8"))
    """Accession of HMM"""
    hmmdesc: str = csfield(CString("utf8"))
    """Description of HMM"""
    sqname: str = csfield(CString("utf8"))
    """Name of target sequence"""
    sqacc: str = csfield(CString("utf8"))
    """Accession of target sequence"""
    sqdesc: str = csfield(CString("utf8"))
    """Description of target sequence"""
    identity: Optional[Tuple[float, int]] = csfield(Computed(lambda ctx: P7AlignmentDisplay.calculate_identity(ctx)))
    """The percentage and count of identical residues between the query and the target."""
    similarity: Optional[Tuple[float, int]] = csfield(
        Computed(lambda ctx: P7AlignmentDisplay.calculate_similarity(ctx))
    )
    """The percentage and count of identical and similar residues between the query and the target."""

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        fields_to_exclude = ["size", "string_presence_flags"]
        old_dict = asdict(self)

        return {key: old_dict[key] for key in old_dict if key not in fields_to_exclude}

    @classmethod
    def calculate_identity(cls, ctx):
        # model mline aseq
        if len(ctx.mline) != len(ctx.aseq):
            return None

        match = "".join(filter(str.isalpha, ctx.mline))
        seq1 = "".join(filter(str.isalpha, ctx.model))
        seq2 = "".join(filter(str.isalpha, ctx.aseq))

        len1 = len(seq1)
        len2 = len(seq2)
        number_of_identical = len(match)
        min_len = min(len1, len2)

        if min_len and number_of_identical:
            return number_of_identical / min_len, number_of_identical
        else:
            return 0, 0

    @classmethod
    def calculate_similarity(cls, ctx):
        if len(ctx.mline) != len(ctx.aseq):
            return None

        match = "".join(ctx.mline.split())
        seq1 = "".join(filter(str.isalpha, ctx.model))
        seq2 = "".join(filter(str.isalpha, ctx.aseq))

        len1 = len(seq1)
        len2 = len(seq2)
        number_of_identical_and_similar = len(match)
        min_len = min(len1, len2)

        if min_len and number_of_identical_and_similar:
            return number_of_identical_and_similar / min_len, number_of_identical_and_similar
        else:
            return 0, 0


@dataclass()
class P7Domain(HmmpgmdModel):
    size: int = csfield(Int32ub)
    """length (in bytes) of the serialized P7_DOMAIN object"""
    ienv: int = csfield(Int64ub)
    """"""
    jenv: int = csfield(Int64ub)
    """"""
    iali: int = csfield(Int64ub)
    """"""
    jali: int = csfield(Int64ub)
    """"""
    iorf: int = csfield(Int64ub)
    """"""
    jorf: int = csfield(Int64ub)
    """"""
    envsc: float = csfield(Float32b)
    """Forward score in envelope ienv..jenv; NATS; without null2 correction"""
    domcorrection: float = csfield(Float32b)
    """Null2 score when calculating a per-domain score; NATS"""
    dombias: float = csfield(Float32b)
    """FLogsum(0, log(bg->omega) + domcorrection): null2 score contribution; NATS"""
    oasc: float = csfield(Float32b)
    """Optimal accuracy score (units: expected # residues correctly aligned)"""
    bitscore: float = csfield(Float32b)
    """Overall score in BITS, null corrected, if this were the only domain in seq"""
    lnP: float = csfield(Float64b)
    """log(P-value) of the bitscore"""
    ievalue: float = csfield(Computed(lambda ctx: math.exp(ctx.lnP) * ctx._._.stats.Z))
    """The independent e-value for the domain"""
    cevalue: float = csfield(Computed(lambda ctx: math.exp(ctx.lnP) * ctx._._.stats.domZ))
    """The conditional e-value for the domain"""
    is_reported: bool = csfield(Int32ub)
    """TRUE if domain meets reporting thresholds"""
    is_included: bool = csfield(Int32ub)
    """TRUE if domain meets inclusion thresholds"""
    scores_per_pos_length: int = csfield(Int32ub)
    """"""
    scores_per_pos: List[int] = csfield(Array(this.scores_per_pos_length, Float32b))
    """Only used by `nhmmer --aliscoresout`; score in BITS that each pos in ali contributes to viterbi score"""
    alignment_display: P7AlignmentDisplay = csfield(DataclassStruct(P7AlignmentDisplay))
    """Domain's P7_ALIDISPLAY structure"""
    display: bool = csfield(Computed(True))
    """Whether to dispay domain"""
    outcompeted: bool = csfield(Computed(False))
    """Whether it is chosen as clan representative"""
    significant: bool = csfield(Computed(False))
    """Whether it is significant in terms of CUT_GA parameter"""
    uniq: int = csfield(Computed(1))
    """"""
    segments: Optional[List[Tuple[int, int]]] = csfield(Computed(None))
    """"""
    predicted_active_sites: Optional[List[Tuple[str, List[int]]]] = csfield(Computed(None))
    """Predicted active sites found after post-processing"""

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        fields_to_exclude = ["size"]
        old_dict = asdict(self)

        return {key: old_dict[key] for key in old_dict if key not in fields_to_exclude}

    def overlaps(self, other: Any, key: str):
        [left, right] = sorted([self, other], key=lambda d: getattr(d, f"i{key}"))

        if getattr(right, f"i{key}") <= getattr(left, f"j{key}"):
            return True

        return False


@dataclass
class P7Hit(HmmpgmdModel):
    index: int = csfield(Computed(lambda ctx: ctx._params.start + ctx._index))
    """Index of the hit (0-based)"""
    size: int = csfield(Int32ub)
    """length (in bytes) of the serialized P7_HIT object"""
    window_length: int = csfield(Int32ub)
    """For later use in e-value computation, when splitting long sequences"""
    sortkey: float = csfield(Float64b)
    """Number to sort by; big is better """
    score: float = csfield(Float32b)
    """Bit score of the sequence (all domains, w/ correction)"""
    pre_score: float = csfield(Float32b)
    """Bit score of sequence before null2 correction"""
    sum_score: float = csfield(Float32b)
    """Bit score reconstructed from sum of domain envelopes """
    bias: float = csfield(Computed(lambda ctx: abs(ctx.score - ctx.pre_score)))
    """Bias"""
    lnP: float = csfield(Float64b)
    """log(P-value) of the score"""
    pre_lnP: float = csfield(Float64b)
    """log(P-value) of the pre_score"""
    sum_lnP: float = csfield(Float64b)
    """log(P-value) of the sum_score"""
    nexpected: float = csfield(Float32b)
    """Posterior expected number of domains in the sequence (from posterior arrays)"""
    nregions: int = csfield(Int32ub)
    """Number of regions evaluated"""
    nclustered: int = csfield(Int32ub)
    """Number of regions evaluated by clustering ensemble of tracebacks"""
    noverlaps: int = csfield(Int32ub)
    """Number of envelopes defined in ensemble clustering that overlap w/ prev envelope"""
    nenvelopes: int = csfield(Int32ub)
    """Number of envelopes handed over for domain definition, null2, alignment, and scoring"""
    ndom: int = csfield(Int32ub)
    """Total number of domains identified in this sequence"""
    flags: Any = csfield(FlagsEnum(Int32ub, P7HitFlags))
    """p7_IS_REPORTED | p7_IS_INCLUDED | p7_IS_NEW | p7_IS_DROPPED"""
    is_reported: bool = csfield(Computed(this.flags.IS_REPORTED))
    """TRUE if hit meets reporting thresholds"""
    is_included: bool = csfield(Computed(this.flags.IS_INCLUDED))
    """TRUE if hit meets inclusion thresholds"""
    is_new: bool = csfield(Computed(this.flags.IS_NEW))
    """TRUE if hit is new"""
    is_dropped: bool = csfield(Computed(this.flags.IS_DROPPED))
    """TRUE if hit is dropped"""
    nreported: int = csfield(Int32ub)
    """Number of domains satisfying reporting thresholding"""
    nincluded: int = csfield(Int32ub)
    """Number of domains satisfying inclusion thresholding"""
    best_domain: int = csfield(Int32ub)
    """Index of best-scoring domain in dcl"""
    seqidx: int = csfield(Int64ub)
    """Unique identifier to track the database sequence from which this hit came"""
    subseq_start: int = csfield(Int64ub)
    """Used to track which subsequence of a full_length target this hit came from"""
    string_presence_flags: Any = csfield(FlagsEnum(Int8ub, P7HitStringPresenceFlags))
    """String presence flags"""
    name: str = csfield(CString("utf8"))
    """Name of the hit"""
    acc: Optional[str] = csfield(If(this.string_presence_flags.ACC_PRESENT, CString("utf8")))
    """Accession of the hit"""
    desc: Optional[str] = csfield(If(this.string_presence_flags.DESC_PRESENT, CString("utf8")))
    """Description of the hit"""
    evalue: float = csfield(Computed(lambda ctx: math.exp(ctx.lnP) * ctx._.stats.Z))
    """E-value of the hit"""
    metadata: Optional[Dict[str, Any]] = csfield(
        If(
            lambda ctx: ctx._params.get("with_metadata", True) and ctx._params.db_conf.metadata_model_class is not None,
            Computed(
                lambda ctx: ctx._params.db_conf.metadata_model_class.model_validate_json(
                    ctx.desc, context={"db_conf": ctx._params.db_conf}
                )
            ),
        )
    )
    """Metadata of the hit"""
    domains: Optional[List[P7Domain]] = csfield(
        If(lambda ctx: ctx._params.get("with_domains", False), Array(this.ndom, DataclassStruct(P7Domain)))
    )
    """Ndom serialized P7_DOMAIN structures"""

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        fields_to_exclude = ["size", "flags", "string_presence_flags", "desc"]
        old_dict = asdict(self)
        old_dict["seqidx"] = int(old_dict["name"])
        return {key: old_dict[key] for key in old_dict if key not in fields_to_exclude}


class Result(BaseModel):
    stats: HmmdSearchStats
    hits: List[P7Hit]

    def post_process(self):
        pass

    @classmethod
    def from_file(
        cls,
        file: os.PathLike,
        start: Optional[int] = None,
        end: Optional[int] = None,
        db_conf: Optional[DatabaseSettings] = None,
        with_metadata=True,
        with_domains=False,
        taxonomy_ids: Optional[List[int]] = None,
        architecture: Optional[str] = None,
        algo: Optional[str] = None,
        id: Optional[str] = None,
    ) -> Tuple["Result", int]:
        stats_format = DataclassStruct(HmmdSearchStats)
        stats = stats_format.parse_file(file)

        if taxonomy_ids or architecture:
            format = Struct(
                "stats" / DataclassStruct(HmmdSearchStats),
                "hits"
                / Array(
                    stats.nhits,
                    Pointer(
                        lambda ctx: ctx.stats.size + ctx.stats.hit_offsets[ctx._params.start + ctx._index],
                        DataclassStruct(P7Hit),
                    ),
                ),
            )

            parsed = format.parse_file(
                file,
                db_conf=db_conf,
                with_domains=with_domains,
                with_metadata=with_metadata,
                start=0,
                algo=algo,
                id=id,
                database=db_conf.name if db_conf else None,
            )

            if taxonomy_ids:
                parsed.hits = [hit for hit in parsed.hits if set(hit.metadata.lineage) & set(taxonomy_ids)]

            if architecture:
                parsed.hits = [hit for hit in parsed.hits if hit.metadata.architecture == architecture]

            total_count = len(parsed.hits)
            parsed.hits = parsed.hits[start:end]

            return parsed, total_count

        start = start or 0
        end = end or stats.nhits

        if start < 0:
            start = 0

        if end > stats.nhits:
            end = stats.nhits

        length = end - start

        format = Struct(
            "stats" / DataclassStruct(HmmdSearchStats),
            "hits"
            / Array(
                length,
                Pointer(
                    lambda ctx: ctx.stats.size + ctx.stats.hit_offsets[ctx._params.start + ctx._index],
                    DataclassStruct(P7Hit),
                ),
            ),
        )

        return (
            format.parse_file(
                file,
                db_conf=db_conf,
                with_domains=with_domains,
                with_metadata=with_metadata,
                start=start,
                algo=algo,
                id=id,
                database=db_conf.name if db_conf else None,
            ),
            stats.nhits,
        )

    @classmethod
    def from_data(
        cls,
        data: bytes,
        start: Optional[int] = None,
        end: Optional[int] = None,
        db_conf: Optional[DatabaseSettings] = None,
        with_metadata=True,
        with_domains=False,
        taxonomy_ids: Optional[List[int]] = None,
        architecture: Optional[str] = None,
        algo: Optional[str] = None,
        id: Optional[str] = None,
    ) -> Tuple["Result", int]:
        stats_format = DataclassStruct(HmmdSearchStats)
        stats = stats_format.parse(data)

        if taxonomy_ids or architecture:
            format = Struct(
                "stats" / DataclassStruct(HmmdSearchStats),
                "hits"
                / Array(
                    stats.nhits,
                    Pointer(
                        lambda ctx: ctx.stats.size + ctx.stats.hit_offsets[ctx._params.start + ctx._index],
                        DataclassStruct(P7Hit),
                    ),
                ),
            )

            parsed = format.parse(
                data,
                db_conf=db_conf,
                with_domains=with_domains,
                with_metadata=with_metadata,
                start=0,
                algo=algo,
                id=id,
                database=db_conf.name if db_conf else None,
            )

            if taxonomy_ids:
                parsed.hits = [hit for hit in parsed.hits if hit.metadata.taxonomy_id in taxonomy_ids]

            if architecture:
                parsed.hits = [hit for hit in parsed.hits if hit.metadata.architecture == architecture]

            total_count = len(parsed.hits)
            parsed.hits = parsed.hits[start:end]

            return parsed, total_count

        start = start or 0
        end = end or stats.nhits

        if start < 0:
            start = 0

        if end > stats.nhits:
            end = stats.nhits

        length = end - start

        format = Struct(
            "stats" / DataclassStruct(HmmdSearchStats),
            "hits"
            / Array(
                length,
                Pointer(
                    lambda ctx: ctx.stats.size + ctx.stats.hit_offsets[ctx._params.start + ctx._index],
                    DataclassStruct(P7Hit),
                ),
            ),
        )

        return (
            format.parse(
                data,
                db_conf=db_conf,
                with_domains=with_domains,
                with_metadata=with_metadata,
                start=start,
                algo=algo,
                id=id,
                database=db_conf.name if db_conf else None,
            ),
            stats.nhits,
        )


def post_process_pfam(result: Result):
    all_domains = [
        {"domain": domain, "hit": hit} for hit in result.hits for domain in filter(lambda d: d.is_included, hit.domains)
    ]

    for flat_domain in all_domains:
        flat_domain["domain"].significant = (
            flat_domain["hit"].score > flat_domain["hit"].metadata.seq_ga
            and flat_domain["domain"].bitscore > flat_domain["hit"].metadata.dom_ga
        )

    for flat_domain in sorted(all_domains, key=lambda d: d["domain"].ievalue):
        if not flat_domain["domain"].display:
            continue

        for other_flat_domain in sorted(all_domains, key=lambda d: d["domain"].ievalue):
            if flat_domain["domain"] == other_flat_domain["domain"]:
                continue

            if flat_domain["domain"].overlaps(other_flat_domain["domain"], "ali"):
                if (
                    flat_domain["hit"].metadata.nested is not None
                    and other_flat_domain["hit"].metadata.identifier in flat_domain["hit"].metadata.nested
                ):
                    continue

                if (
                    flat_domain["hit"].metadata.clan is not None
                    and other_flat_domain["hit"].metadata.clan is not None
                    and flat_domain["hit"].metadata.clan == other_flat_domain["hit"].metadata.clan
                ):
                    other_flat_domain["domain"].display = False
                    other_flat_domain["domain"].outcompeted = True

    for i, flat_domain in enumerate(sorted(all_domains, key=lambda d: d["domain"].iali)):
        if not flat_domain["domain"].display:
            continue

        for other_flat_domain in sorted(all_domains, key=lambda d: d["domain"].iali)[i + 1:]:
            if not other_flat_domain["domain"].display:
                continue

            if flat_domain["domain"].overlaps(other_flat_domain["domain"], "ali"):
                if (
                    flat_domain["hit"].metadata.nested is None
                    or other_flat_domain["hit"].metadata.identifier not in flat_domain["hit"].metadata.nested
                ):
                    continue

                if flat_domain["domain"].segments is None:
                    flat_domain["domain"].segments = [(flat_domain["domain"].ienv, flat_domain["domain"].jenv)]

                j = 0

                while j < len(flat_domain["domain"].segments):
                    segment = flat_domain["domain"].segments[j]
                    unit = {"ienv": segment[0], "jenv": segment[1]}

                    if other_flat_domain["domain"].overlaps(unit, "env"):
                        if segment[0] < other_flat_domain["domain"].ienv:
                            segment[1] = other_flat_domain["domain"].ienv - 1
                            if unit["jenv"] > other_flat_domain["domain"].jenv + 1:
                                flat_domain["domain"].segments.append(
                                    (other_flat_domain["domain"].jenv + 1, unit["jenv"])
                                )
                        elif segment[1] > other_flat_domain["domain"].jenv:
                            if segment[1] > other_flat_domain["domain"].jenv + 1:
                                segment[1] = other_flat_domain["domain"].jenv + 1
                            else:
                                flat_domain["domain"].segments.pop(j)
                        else:
                            flat_domain["domain"].segments.pop(j)
                    else:
                        j += 1

                if len(flat_domain["domain"].segments) < 1:
                    flat_domain["domain"].display = False


def predict_active_sites(result: Result):
    for hit in result.hits:
        if hit.metadata.active_sites is None:
            continue

        for domain in hit.domains:
            seq = list(domain.alignment_display.aseq)
            subpattern = False
            matched_patterns = {}

            for source, patterns in hit.metadata.active_site:
                # Check this isn't a subpattern of a bigger pattern already found on the sequence
                # Patterns were added to @{$self->{_act_site_data}} in order of size (longest pattern first)
                for asp in matched_patterns.keys():
                    different = False
                    for pattern in patterns:  # eg 'S23 T34 S56'
                        if m := re.match(r"(\S)(\d+)", pattern):  # eg 'S23'
                            residue, hmm_position = m.groups()
                            hmm_position = int(hmm_position)
                            if hmm_position in matched_patterns[asp]:
                                continue
                            else:
                                different = True
                                break
                    if not different:
                        subpattern = True
                        break
                if subpattern:
                    continue

                match = []

                # Set up the counters so we know which position we are at
                hmm_counter = domain.alignment_display.hmmfrom
                residue_counter = domain.alignment_display.sqfrom

                # Now check to see if pattern is in this sequence
                # Put pattern into a dict
                as_positions = {}
                in_seq = False
                for pattern in patterns:  # eg 'S23 T34 S56'
                    if m := re.match(r"(\S)(\d+)", pattern):  # eg 'S23'
                        residue, hmm_position = m.groups()
                        hmm_position = int(hmm_position)
                        as_positions[hmm_position] = residue
                        if domain.alignment_display.hmmfrom <= hmm_position <= domain.alignment_display.hmmto:
                            in_seq = True
                if not in_seq:  # Active site residue positions are not located in this region
                    continue

                # Look for the active site pattern in the sequence
                # Active site residues will be in match positions only
                for aa in seq:
                    if re.match(r"[A-Z]", aa):  # Uppercase residues are match states
                        if hmm_counter in as_positions:  # It's an active site residue position
                            if aa == as_positions[hmm_counter]:  # Does it have the correct aa at that position
                                match.append(residue_counter)
                                matched_patterns.setdefault(" ".join(patterns), {})[hmm_counter] = True
                                del as_positions[hmm_counter]
                                if not as_positions:
                                    break
                            else:
                                break
                        hmm_counter += 1
                        residue_counter += 1
                    elif aa == "-":  # Deletion in a match state position
                        if hmm_counter in as_positions:
                            break
                        hmm_counter += 1
                    elif re.match(r"[a-z]", aa):  # Lowercase residues are not match state positions
                        if hmm_counter in as_positions:
                            break
                        residue_counter += 1
                    elif aa == ".":  # '.' is not a match state position
                        if hmm_counter in as_positions:
                            break
                    else:
                        raise ValueError(f"Unrecognised character [{aa}] in {domain.alignment_display.aseq}")

                if as_positions:
                    matched_patterns.pop(" ".join(patterns), None)
                else:
                    if domain.predicted_active_sites is None:
                        domain.predicted_active_sites = [(source, match)]
                    else:
                        domain.predicted_active_sites.append((source, match))
