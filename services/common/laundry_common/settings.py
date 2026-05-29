import os


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


def int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


def float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None or value == "" else float(value)


def pg_dsn() -> str:
    host = env("POSTGRES_HOST", "postgres")
    port = env("POSTGRES_PORT", "5432")
    db = env("POSTGRES_DB", "laundry")
    user = env("POSTGRES_USER", "laundry")
    password = env("POSTGRES_PASSWORD", "laundry")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def kafka_servers() -> str:
    return env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def redis_url() -> str:
    host = env("REDIS_HOST", "redis")
    port = env("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/0"

