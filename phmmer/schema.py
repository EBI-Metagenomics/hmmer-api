from ninja import Field
from search.schema import CutOffSchema, GapPenaltiesSchema


class PhmmerJobSchema(CutOffSchema, GapPenaltiesSchema):
    seq: str = Field(..., min_length=1)
    seqdb: str
