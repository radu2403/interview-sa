from dataclasses import dataclass, field
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class ReadDAOConfig:
    path: str = None
    schema: StructType = None