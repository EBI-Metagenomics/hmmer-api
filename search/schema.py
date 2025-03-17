from typing import Literal, Optional
from ninja import Schema, ModelSchema, Field
from pydantic import UUID4, model_validator
from .models import HmmerJob


# Response Schemas
class MessageSchema(Schema):
    message: str


class HmmerJobCreatedSchema(Schema):
    id: UUID4 = Field(..., description="The id of the job")
    status: str = Field(..., description="The status of the job")
    status_url: str = Field(..., description="URL to get the status of the job")


class HmmerJobStatusSchema(Schema):
    id: UUID4 = Field(..., description="The id of the job")
    status: str = Field(..., description="The status of the job")
    result_url: Optional[str] = Field(..., description="URL to get the result of the job")
    error_message: Optional[str] = Field(..., description="The error message of the job")


# Helper Schemas for defining complex request schemas
class CutOffSchema(Schema):
    threshold: Literal["evalue", "bitscore"] = "evalue"
    incE: Optional[float] = Field(0.01, gt=0, le=10)
    incdomE: Optional[float] = Field(0.03, gt=0, le=10)
    E: Optional[float] = Field(1.0, gt=0, le=10)
    domE: Optional[float] = Field(1.0, gt=0, le=10)
    incT: Optional[float] = Field(25.0, gt=0)
    incdomT: Optional[float] = Field(22.0, gt=0)
    T: Optional[float] = Field(7.0, gt=0)
    domT: Optional[float] = Field(5.0, gt=0)

    @model_validator(mode="after")
    def clean_threshold_fields(self):
        if self.threshold == "evalue":
            self.incT = None
            self.incdomT = None
            self.T = None
            self.domT = None
        else:
            self.incE = None
            self.incdomE = None
            self.E = None
            self.domE = None
        return self


class GapPenaltiesSchema(Schema):
    popen: Optional[float] = Field(0.02, ge=0, lt=0.5)
    pextend: Optional[float] = Field(0.4, ge=0, lt=1.0)
    mx: Optional[Literal["BLOSUM62", "BLOSUM45", "BLOSUM90", "PAM30", "PAM70", "PAM250"]] = "BLOSUM62"
