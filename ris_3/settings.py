import os
from typing import Final


POSTGRES_URI: Final[str] = os.getenv(
    'POSTGRES_URI',
    'postgresql://postgres:postgres@postgres:5432/ris_3'
)
CLICKHOUSE_URI: Final[str] = os.getenv(
    'CLICKHOUSE_URI',
    'clickhouse+native://clickhouse:clickhouse@clickhouse:9000/default'
)
