import operator
from contextlib import asynccontextmanager
from typing import Annotated
import os

import fastapi
import logfire
from aiobotocore.client import AioBaseClient
from environs import Env
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from sqlalchemy import cast, Date, delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

import schemas
from botocore_client import get_async_client
from database import create_all_tables, get_async_session
from models import CurrencyRate
from schemas import available_output_formats, Request, Response

# Загружаем env
env = Env()
env.read_env()

DBSession = Annotated[AsyncSession, Depends(get_async_session)]
BotoClient = Annotated[AioBaseClient, Depends(get_async_client)]
backup_request = Request(isBackup=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_all_tables()
    yield


async def api_token(
    token: Annotated[str, Depends(APIKeyHeader(name="API-Token"))]
) -> None:
    expected_token = env("API_TOKEN", "default_token_for_development")
    if token != expected_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)


app = FastAPI(
    lifespan=lifespan,
    title="Currency Rates Database API",
    version="0.1.0",
    contact={
        "name": "Currency Rates API",
        "email": "your-email@example.com"
    }
)

# Настройка Logfire только если токен указан
try:
    logfire_token = env("LOGFIRE_TOKEN")
    if logfire_token:
        logfire.configure(token=logfire_token, service_name="Currency-API")
        logfire.instrument_fastapi(app, capture_headers=True)
except:
    print("Logfire not configured, continuing without logging")


@app.get("/")
async def redirect_from_root_to_docs():
    return RedirectResponse(url="/docs")


@app.post(
    "/currency-rates",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_201_CREATED
)
async def post_currency_rates(
    rates: list[schemas.CurrencyRate], session: DBSession, client: BotoClient
):
    # Используем logfire только если настроен
    if logfire_token:
        with logfire.span("Create new entries"):
            rates = [CurrencyRate(**rate.model_dump()) for rate in rates]
        with logfire.span("Add entries and commit"):
            session.add_all(rates)
            await session.commit()
        with logfire.span("Make a database backup"):
            await get_currency_rates(backup_request, session, client)
    else:
        rates = [CurrencyRate(**rate.model_dump()) for rate in rates]
        session.add_all(rates)
        await session.commit()
        await get_currency_rates(backup_request, session, client)


@app.get("/currency-rates")
async def get_currency_rates(
    r: Annotated[Request, Query()], session: DBSession, client: BotoClient
):
    if logfire_token:
        with logfire.span("Select entries"):
            dates = cast(CurrencyRate.date, Date)
            clauses = [(r.startDate, operator.ge), (r.endDate, operator.le)]
            clauses = [func(dates, date) for date, func in clauses if date]
            columns = [CurrencyRate.date, CurrencyRate.letter_code]
            statement = select(CurrencyRate).where(*clauses).order_by(*columns)
            result = await session.execute(statement)

        with logfire.span("Pick a handler, handle entries, return a response"):
            if not (scalars := result.scalars().all()):
                return Response(**r.model_dump(), comment="No results")

            handler_class = available_output_formats[r.outputFormat]
            handler = handler_class(scalars, client, r.isBackup)
            await handler.upload_contents()

            if r.isBackup:
                return fastapi.Response(status_code=status.HTTP_204_NO_CONTENT)

            shortened_presigned_url = await handler.generate_url()
            return Response(**r.model_dump(), url=shortened_presigned_url)
    else:
        # Без logfire
        dates = cast(CurrencyRate.date, Date)
        clauses = [(r.startDate, operator.ge), (r.endDate, operator.le)]
        clauses = [func(dates, date) for date, func in clauses if date]
        columns = [CurrencyRate.date, CurrencyRate.letter_code]
        statement = select(CurrencyRate).where(*clauses).order_by(*columns)
        result = await session.execute(statement)

        if not (scalars := result.scalars().all()):
            return Response(**r.model_dump(), comment="No results")

        handler_class = available_output_formats[r.outputFormat]
        handler = handler_class(scalars, client, r.isBackup)
        await handler.upload_contents()

        if r.isBackup:
            return fastapi.Response(status_code=status.HTTP_204_NO_CONTENT)

        shortened_presigned_url = await handler.generate_url()
        return Response(**r.model_dump(), url=shortened_presigned_url)


@app.delete(
    "/currency-rates",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_currency_rates(
    delete_ids: list[int], session: DBSession, client: BotoClient
):
    if logfire_token:
        with logfire.span("Delete entries and commit"):
            statement = delete(CurrencyRate).where(CurrencyRate.id.in_(delete_ids))
            await session.execute(statement)
            await session.commit()
        with logfire.span("Make a database backup"):
            await get_currency_rates(backup_request, session, client)
    else:
        statement = delete(CurrencyRate).where(CurrencyRate.id.in_(delete_ids))
        await session.execute(statement)
        await session.commit()
        await get_currency_rates(backup_request, session, client)


@app.get("/currency-rates/latest")
async def get_latest_rates(
    currency_codes: Annotated[list[str] | None, Query()] = None,
    session: DBSession = Depends(get_async_session)
):
    """
    Get latest currency rates for specified currencies.
    If no currencies specified, returns all latest rates.
    """
    if logfire_token:
        with logfire.span("Get latest rates"):
            # Subquery to get latest date for each currency
            subquery = (
                select(
                    CurrencyRate.letter_code,
                    CurrencyRate.date,
                    CurrencyRate.exchange_rate
                )
                .order_by(
                    CurrencyRate.letter_code,
                    CurrencyRate.date.desc()
                )
                .distinct(CurrencyRate.letter_code)
                .subquery()
            )

            # Main query
            statement = select(CurrencyRate).join(
                subquery,
                (CurrencyRate.letter_code == subquery.c.letter_code) &
                (CurrencyRate.date == subquery.c.date) &
                (CurrencyRate.exchange_rate == subquery.c.exchange_rate)
            )

            if currency_codes:
                statement = statement.where(CurrencyRate.letter_code.in_(currency_codes))

            result = await session.execute(statement)
            rates = result.scalars().all()
    else:
        # Без logfire
        subquery = (
            select(
                CurrencyRate.letter_code,
                CurrencyRate.date,
                CurrencyRate.exchange_rate
            )
            .order_by(
                CurrencyRate.letter_code,
                CurrencyRate.date.desc()
            )
            .distinct(CurrencyRate.letter_code)
            .subquery()
        )

        statement = select(CurrencyRate).join(
            subquery,
            (CurrencyRate.letter_code == subquery.c.letter_code) &
            (CurrencyRate.date == subquery.c.date) &
            (CurrencyRate.exchange_rate == subquery.c.exchange_rate)
        )

        if currency_codes:
            statement = statement.where(CurrencyRate.letter_code.in_(currency_codes))

        result = await session.execute(statement)
        rates = result.scalars().all()

    return {
        "rates": [
            {
                "digital_code": rate.digital_code,
                "letter_code": rate.letter_code,
                "units": rate.units,
                "currency_name": rate.currency_name,
                "exchange_rate": rate.exchange_rate,
                "date": rate.date.isoformat(),
                "source": rate.source
            }
            for rate in rates
        ]
    }


# Создаем папку для экспортов при запуске
os.makedirs("./exports", exist_ok=True)