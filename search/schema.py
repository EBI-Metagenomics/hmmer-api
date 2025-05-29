from ninja import Schema


class ValidationErrorDetailSchema(Schema):
    type: str
    loc: list[str]
    msg: str


class ValidationErrorSchema(Schema):
    detail: list[ValidationErrorDetailSchema]
