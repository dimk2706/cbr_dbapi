import logfire
import uvicorn
from environs import Env

# Загружаем env
env = Env()
env.read_env()

# Настройка Logfire только если токен указан
logfire_token = env("LOGFIRE_TOKEN", None)
if logfire_token:
    logfire.configure(token=logfire_token, service_name="Currency-API")
    logfire.install_auto_tracing(
        modules=["main", "handlers"],
        min_duration=0
    )
else:
    print("Logfire not configured - continuing without logging")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=env("ECS_PRIVATE_IP", "0.0.0.0"),
        port=env.int("ECS_PORT", 8000)
    )