import os


def get_environment() -> str:
    return os.getenv("PIPELINE_ENV", "prod")


def get_catalog() -> str:
    return "workspace"


def get_schema(layer: str = "gold") -> str:
    return "mention_dw"
