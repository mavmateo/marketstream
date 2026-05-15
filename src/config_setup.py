import os
from functools import lru_cache
from pathlib import Path
import yaml
from pydantic import BaseModel, field_validator


class PipelineConfig(BaseModel):
    name: str
    version: str
    environment: str


class LoggingConfig(BaseModel):
    level : str
    log_to_file : bool
    log_path : str
    max_bytes : int
    backup_count : int


class AppConfig(BaseModel):
    pipeline: PipelineConfig
    logging: LoggingConfig

    class Config:
        populate_by_name = True




CONFIG_PATH = os.getenv(
    "PIPELINE_CONFIG",
    str(Path(__file__).parent.parent / "src"/ "config" / "settings.yaml"),
)


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """Load config once and cache. Override path via PIPELINE_CONFIG env var."""
    with open(CONFIG_PATH, "r") as f:
        raw = yaml.safe_load(f)

    # 'schema' is a reserved name in Pydantic v2 — rename key
    #raw["schema_"] = raw.pop("schema")
    return AppConfig(**raw)