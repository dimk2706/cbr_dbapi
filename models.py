from datetime import datetime

from sqlalchemy import DateTime, Integer, MetaData, String, Text, Float, Date
from sqlalchemy.orm import declarative_base, Mapped, mapped_column


Base = declarative_base(metadata=MetaData(schema="currency-schema"))


class CurrencyRate(Base):
    __tablename__ = "currency_rates"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    digital_code: Mapped[str] = mapped_column(String(3), nullable=False)
    letter_code: Mapped[str] = mapped_column(String(3), nullable=False)
    units: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_name: Mapped[str] = mapped_column(String(255), nullable=False)
    exchange_rate: Mapped[float] = mapped_column(Float, nullable=False)
    date: Mapped[datetime] = mapped_column(Date, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default='cbr.ru')


currency_columns = CurrencyRate.__table__.columns.keys()