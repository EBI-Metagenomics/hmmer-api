import os
import io
import re
from enum import IntEnum
from ctypes import CDLL, byref, c_char_p, c_void_p, c_int, Structure, POINTER, c_double, c_int64, create_string_buffer
from ctypes.util import find_library
from tempfile import NamedTemporaryFile
from typing import List, Type, Optional
from hmmerapi.config import DatabaseSettings
from pyhmmer.easel import MSAFile, SequenceFile, Alphabet
from pyhmmer.plan7 import Background, Builder


class MSAFormat(IntEnum):
    STOCKHOLM = 101  # Stockholm format, interleaved
    PFAM = 102  # Pfam/Rfam one-line-per-seq Stockholm format
    A2M = 103  # UCSC SAM's fasta-like a2m format
    PSIBLAST = 104  # NCBI PSI-BLAST alignment format
    SELEX = 105  # old SELEX format (largely obsolete)
    AFA = 106  # aligned FASTA format
    CLUSTAL = 107  # CLUSTAL format
    CLUSTALLIKE = 108  # CLUSTAL-like formats (MUSCLE, PROBCONS)
    PHYLIP = 109  # interleaved PHYLIP format
    PHYLIPS = 110  # sequential PHYLIP format


class FILE(Structure):
    pass


class ESL_MSA(Structure):
    _fields_ = [
        ("aseq", POINTER(c_char_p)),
        ("sqname", POINTER(c_char_p)),
        ("wgt", POINTER(c_double)),
        ("alen", c_int64),
        ("nseq", c_int),
        ("flags", c_int),
        ("abc", c_void_p),
        ("ax", POINTER(c_void_p)),
        ("name", c_char_p),
        ("desc", c_char_p),
        ("acc", c_char_p),
        ("au", c_char_p),
        ("ss_cons", c_char_p),
        ("sa_cons", c_char_p),
        ("pp_cons", c_char_p),
        ("rf", c_char_p),
        ("mm", c_char_p),
        # Array of string pointers (char **)
        ("sqacc", POINTER(c_char_p)),
        ("sqdesc", POINTER(c_char_p)),
        ("ss", POINTER(c_char_p)),
        ("sa", POINTER(c_char_p)),
        ("pp", POINTER(c_char_p)),
    ]


ESL_MSA_p = Type[POINTER(ESL_MSA)]


metadata_regex = re.compile(r"\[subseq\sfrom\]\s(.+)$")


hmmer_path = find_library("hmmer")
hmmer = CDLL(hmmer_path)

libc_path = find_library("c")
libc = CDLL(libc_path)


def set_msa_info(msa: ESL_MSA_p, db_conf: DatabaseSettings):
    metadata_class = db_conf.metadata_model_class

    if metadata_class is None:
        return

    n_seqs = msa.contents.nseq

    for i in range(n_seqs):
        seq_name_encoded = msa.contents.sqname[i]
        seq_name = seq_name_encoded.decode()
        _, range_part = seq_name.split("/")

        seq_desc_encoded = msa.contents.sqdesc[i]
        seq_desc = seq_desc_encoded.decode()
        match = metadata_regex.match(seq_desc)

        if match is None:
            return

        metadata = metadata_class.model_validate_json(match.group(1), context={"db_conf": db_conf})
        new_name = f"{metadata.accession}/{range_part}"
        new_desc = f"[subseq from] {metadata.description}"
        hmmer.esl_msa_SetSeqName(msa, i, create_string_buffer(new_name.encode()), len(new_name))
        hmmer.esl_msa_SetSeqDescription(msa, i, create_string_buffer(new_desc.encode()), len(new_desc))


def construct_align(
    hits: os.PathLike,
    hmm_input: str,
    include: List[int] = [],
    exclude: List[int] = [],
    exclude_all: bool = False,
    db_conf: Optional[DatabaseSettings] = None,
):

    hmm_buffer = c_char_p(hmm_input.encode())
    hmm_file = c_void_p()

    return_code = hmmer.p7_hmmfile_OpenBuffer(hmm_buffer, len(hmm_input), byref(hmm_file))

    if return_code:
        raise Exception(f"Failed to read hmm: {return_code}")

    hmm = c_void_p()
    abc = c_void_p()

    return_code = hmmer.p7_hmmfile_Read(hmm_file, byref(abc), byref(hmm))

    if return_code:
        raise Exception(f"Failed to load hmm: {return_code}")

    with open(hits, mode="rb") as hits_fh:
        data = c_char_p(hits_fh.read())

    incl = (c_int * len(include))(*include)
    excl = (c_int * len(exclude))(*exclude)
    incl_size = len(include)
    excl_size = len(exclude)
    excl_all = c_int(1 if exclude_all else 0)
    ret_msa = POINTER(ESL_MSA)()

    return_code = hmmer.hmmpgmd2msa(data, hmm, None, incl, incl_size, excl, excl_size, excl_all, byref(ret_msa))

    if return_code:
        raise Exception(f"Failed to build msa: {return_code}")

    if db_conf is not None:
        set_msa_info(ret_msa, db_conf)

    hmmer.p7_hmmfile_Close(hmm_file)
    hmmer.p7_hmm_Destroy(hmm)
    hmmer.esl_alphabet_Destroy(abc)

    return ret_msa


def hmm_from_hmmpgmd(
    hits: os.PathLike,
    hmm_input: str,
    include: List[int] = [],
    exclude: List[int] = [],
    exclude_all: bool = False,
):
    ret_msa = construct_align(hits, hmm_input, include, exclude, exclude_all)

    return_code = hmmer.esl_msa_SetName(ret_msa, c_char_p("jackhmmer".encode()), -1)

    if return_code:
        raise Exception(f"Failed to set msa name: {return_code}")

    tempfile = NamedTemporaryFile(delete=False)

    libc.fopen.argtypes = [c_char_p, c_char_p]
    libc.fopen.restype = POINTER(FILE)

    fp = libc.fopen(tempfile.name.encode(), b"w")

    if fp is None:
        raise Exception("Unable to open temp file")

    return_code = hmmer.esl_msafile_Write(fp, ret_msa, 101)
    libc.fflush(fp)
    libc.fclose(fp)

    if return_code:
        raise Exception(f"Failed to write msa to file: {return_code}")

    hmmer.esl_msa_Destroy(ret_msa)

    with MSAFile(tempfile.name, digital=True) as mf:
        msa = mf.read()

    alphabet = Alphabet.amino()
    background = Background(alphabet)
    builder = Builder(alphabet)

    hmm, _, _ = builder.build_msa(msa, background)

    hmm_fh = io.BytesIO()
    hmm.write(hmm_fh, binary=False)
    bytes = hmm_fh.getvalue()

    return bytes.decode()


def msa_from_hmmpgmd(
    hits: os.PathLike, hmm_input: str, format: str, db_conf: DatabaseSettings, include: List[int] = []
):
    try:
        format_enum = MSAFormat[format.upper()]
    except KeyError:
        raise Exception("Unsupported MSA format")

    ret_msa = construct_align(hits, hmm_input, include=include, exclude=[], exclude_all=bool(include), db_conf=db_conf)

    tempfile = NamedTemporaryFile(mode="rt", delete=False)

    libc.fopen.argtypes = [c_char_p, c_char_p]
    libc.fopen.restype = POINTER(FILE)

    fp = libc.fopen(tempfile.name.encode(), b"w")

    if fp is None:
        raise Exception("Unable to open temp file")

    return_code = hmmer.esl_msafile_Write(fp, ret_msa, format_enum.value)
    libc.fflush(fp)
    libc.fclose(fp)

    if return_code:
        raise Exception("Failed to write msa to file")

    hmmer.esl_msa_Destroy(ret_msa)

    return tempfile.read()


def msa_to_hmm(input: str):
    with MSAFile(io.BytesIO(input.encode()), digital=True) as msa_fh:
        alphabet = msa_fh.guess_alphabet()
        msa = msa_fh.read()

        if msa is None:
            raise ValueError("MSA input is empty")

        if msa.name is None:
            msa.name = b"Query"

        builder = Builder(alphabet)
        background = Background(alphabet)
        hmm, _, _ = builder.build_msa(msa, background)

        hmm_fh = io.BytesIO()
        hmm.write(hmm_fh, binary=False)
        bytes = hmm_fh.getvalue()

        return bytes.decode()


def seq_to_hmm(input: str):
    seq = SequenceFile.parse(input.encode(), format="fasta")

    if seq is None:
        raise ValueError("Sequence input is empty")

    alphabet = Alphabet.amino()
    seq = seq.digitize(alphabet)

    builder = Builder(alphabet)
    background = Background(alphabet)
    hmm, _, _ = builder.build(seq, background)

    hmm_fh = io.BytesIO()
    hmm.write(hmm_fh, binary=False)
    bytes = hmm_fh.getvalue()

    return bytes.decode()
