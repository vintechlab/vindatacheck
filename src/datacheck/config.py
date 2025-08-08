import argparse
import os
import yaml
from pydantic import BaseModel
from typing import Literal, Union

from pyspark import SparkFiles


class GlobalConfig(BaseModel):
    ignore_data_types: str = ""
    ignore_columns: str = ""
    filter_data: str = ""
    created_at_column: str = "created_at"
    updated_at_column: str = "updated_at"


class MySQLConfig(GlobalConfig):
    type: Literal["mysql"]
    url: str
    database: str
    table_name: str
    mysql_user: str = os.getenv("MYSQL_USER", "")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "")


class BigQueryConfig(GlobalConfig):
    type: Literal["bigquery"]
    project: str
    materializationProject: str
    materializationDataset: str
    dataset: str
    table_name: str


DataSourceConfig = Union[MySQLConfig, BigQueryConfig]


class Config(BaseModel):
    source: DataSourceConfig
    target: DataSourceConfig


def build_config(config: dict, global_config: GlobalConfig) -> DataSourceConfig:
    merged_config = {**global_config.model_dump(), **config}

    match config["type"]:
        case "mysql":
            return MySQLConfig(**merged_config)
        case "bigquery":
            return BigQueryConfig(**merged_config)
        case _:
            raise ValueError(f"Invalid data source type: {config['type']}")


def parse_args():
    parser = argparse.ArgumentParser(description="Data Check")
    parser.add_argument("--config-file", type=str, required=True, help="Config file")
    args = parser.parse_args()

    with open(SparkFiles.get(args.config_file), "r") as stream:
        raw = yaml.safe_load(stream)

    global_config = GlobalConfig(**raw["global_config"])

    return Config(
        source=build_config(raw["source"], global_config),
        target=build_config(raw["target"], global_config),
    )
