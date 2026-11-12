from pydantic import BaseModel, Field, field_validator, model_validator


class HealthResponse(BaseModel):
    service: str
    status: str


class BookResponse(BaseModel):
    book_id: str
    name: str
    description: str | None = None
    expected_asset_class: str
    is_active: bool


class BookListResponse(BaseModel):
    books: list[BookResponse]


class BookCreate(BaseModel):
    name: str = Field(..., min_length=1)
    expected_asset_class: str = Field(..., min_length=1)
    description: str = ""

    @field_validator("description", mode="before")
    @classmethod
    def blank_description(cls, value):
        if value is None:
            return ""
        return value


class BookUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    expected_asset_class: str | None = None
    is_active: bool | None = None

    @model_validator(mode="after")
    def require_a_field(self):
        if not self.model_fields_set:
            raise ValueError("Invalid JSON payload")
        return self


class CreateBooksResponse(BaseModel):
    message: str
    book_ids: list[str]


class MessageResponse(BaseModel):
    message: str
