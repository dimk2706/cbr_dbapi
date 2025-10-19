from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack

from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession
from botocore.client import Config
from environs import Env

# Загружаем env
env = Env()
env.read_env()

# Параметры для S3 (опционально)
try:
    params = {
        "service_name": "s3",
        "aws_access_key_id": env("OBS_ACCESS_KEY"),
        "aws_secret_access_key": env("OBS_SECRET_KEY"),
        "region_name": env("OBS_REGION"),
        "endpoint_url": env("OBS_ENDPOINT"),
        "config": Config(s3={"addressing_style": "virtual"})
    }
    S3_ENABLED = True
except Exception as e:
    print(f"S3 disabled: {e}")
    S3_ENABLED = False
    params = {}


async def create_async_client(session: AioSession, exit_stack: AsyncExitStack):
    if not S3_ENABLED:
        return None
    context_manager = session.create_client(**params)
    client = await exit_stack.enter_async_context(context_manager)
    return client


async def get_async_client() -> AsyncGenerator[AioBaseClient, None]:
    if not S3_ENABLED:
        yield None
        return
        
    session = AioSession()
    async with AsyncExitStack() as exit_stack:
        client = await create_async_client(session, exit_stack)
        yield client