import inspect
from datetime import date, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

import handlers


available_output_formats = {
    obj.extension: obj
    for name, obj in inspect.getmembers(handlers)
    if name.endswith("Maker")
}
outputFormats = Literal[tuple(available_output_formats)]
date_value = str | date | None


class CurrencyRate(BaseModel):
    digital_code: str
    letter_code: str
    units: int
    currency_name: str
    exchange_rate: float
    date: str
    timestamp: str
    source: str = 'cbr.ru'

    @field_validator("date", mode="after")
    @classmethod
    def handle_date(cls, value: str) -> date:
        return datetime.strptime(value, "%d.%m.%Y").date()

    @field_validator("timestamp", mode="after")
    @classmethod
    def handle_timestamp(cls, value: str) -> datetime:
        return datetime.fromisoformat(value)


class Request(BaseModel):
    startDate: date_value = None
    endDate: date_value = None
    outputFormat: outputFormats = "parquet"  # type: ignore
    isBackup: Annotated[bool, Field(exclude=True)] = False

    @field_validator("startDate", "endDate", mode="before")
    @classmethod
    def handle_dates(cls, value: date_value) -> date_value:
        if value is None:
            return None
        if isinstance(value, str):  # for a request
            return datetime.strptime(value, "%Y%m%d").date()
        if isinstance(value, date):  # for a response
            return value.strftime("%Y-%m-%d")


class Response(Request):
    url: str | None = None
    comment: str | None = None