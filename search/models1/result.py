import io
import os
from functools import cached_property
from construct import Float64b, Float32b, Int8ub, Int64ub, Int32ub, Array, this, FlagsEnum, CString, If, Tell
from construct_typed import DataclassMixin, DataclassStruct, csfield, FlagsEnumBase, EnumBase
from pydantic import BaseModel, Field, computed_field, FilePath, ConfigDict
from pydantic.dataclasses import dataclass
from typing import List, Optional, TypeVar, Type, Any, Union

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

        with open(file, mode="rb") as f:
            f.seek(offset)
            return format.parse_stream(f, **kwargs)

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
    Z_setby: ZSetByEnum = csfield(Int8ub)
    """How Z was set"""
    domZ_setby: ZSetByEnum = csfield(Int8ub)
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


@dataclass
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


@dataclass
class P7Hit(HmmpgmdModel):
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
    domains: Optional[List[P7Domain]] = csfield(
        If(lambda ctx: ctx._.get("with_domains", False), Array(this.ndom, DataclassStruct(P7Domain)))
    )
    """Ndom serialized P7_DOMAIN structures"""


class Result(BaseModel):
    binary_file: FilePath = Field(exclude=True)

    @computed_field
    @cached_property
    def stats(self) -> HmmdSearchStats:
        return HmmdSearchStats.from_file(self.binary_file)

    @computed_field
    @cached_property
    def hits(self) -> List[P7Hit]:
        return self._hits()

    def _hits(self, index: Optional[Union[int, slice]], with_domains=False) -> List[P7Hit]:
        initial_offset = self.stats.size

        if index is None:
            with open(self.binary_file, mode="rb") as fh:
                fh.seek(initial_offset)
                return [
                    P7Hit.from_file(self.binary_file, offset=0, with_domains=with_domains)
                    for _ in range(self.stats.nhits)
                ]
        elif isinstance(index, int):
            offset = self.stats.hit_offsets[index]
            return [P7Hit.from_file(self.binary_file, offset=offset + initial_offset, with_domains=with_domains)]
        elif isinstance(index, slice):
            start = index.start or 0
            stop = index.stop or self.stats.nhits
            step = index.step or 1
            offsets = self.stats.hit_offsets[start:stop:step]
            return [
                P7Hit.from_file(self.binary_file, offset=offset + initial_offset, with_domains=with_domains)
                for offset in offsets
            ]

    class Config:
        json_schema_mode_override = "serialization"
