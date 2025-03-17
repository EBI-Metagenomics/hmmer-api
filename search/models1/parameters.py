from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class MXEnum(str, Enum):
    BLOSUM62 = "BLOSUM62"
    BLOSUM45 = "BLOSUM45"
    BLOSUM90 = "BLOSUM90"
    PAM30 = "PAM30"
    PAM70 = "PAM70"
    PAM250 = "PAM250"


class ThresholdEnum(str, Enum):
    EVALUE = "evalue"
    BITSCORE = "bitscore"


class SearchParameters(BaseModel):
    threshold: ThresholdEnum = Field(default=ThresholdEnum.EVALUE)
    E: float = Field(default=1.0)
    T: float = Field(default=7.0)
    domE: float = Field(default=1.0)
    domT: float = Field(default=5.0)
    incE: float = Field(default=0.01)
    incdomE: float = Field(default=0.03)
    incT: float = Field(default=25.0)
    incdomT: float = Field(default=22.0)
    popen: float = Field(default=0.02)
    pextend: float = Field(default=0.4)
    mx: MXEnum = Field(default=MXEnum.BLOSUM62)

    model_config = ConfigDict(extra="ignore")
