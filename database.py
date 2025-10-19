from collections.abc import AsyncGenerator

from environs import Env
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)

from models import Base

# Загружаем env
env = Env()
env.read_env()

# Используем SQLite по умолчанию, если POSTGRES_URL не указан
database_url = env("POSTGRES_URL", "sqlite+aiosqlite:///./currency.db")
engine = create_async_engine(database_url)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


async def create_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)