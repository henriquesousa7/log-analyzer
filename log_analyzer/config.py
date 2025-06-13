import os
from pathlib import Path

# Tabelas
BRONZE_TABLE = os.getenv("BRONZE_TABLE")
SILVER_TABLE = os.getenv("SILVER_TABLE")
GOLD_TABLE = os.getenv("GOLD_TABLE")

# Parquet and Log Path
PARQUETS_PATH = Path(os.getenv("PARQUETS_PATH"))
LOG_PATH = os.getenv("LOG_PATH")

# Caminhos com nomes das tabelas incluídos
BRONZE_PATH = Path(PARQUETS_PATH / "bronze" / BRONZE_TABLE).resolve().as_posix()
SILVER_PATH = Path(PARQUETS_PATH / "silver" / SILVER_TABLE).resolve().as_posix()
GOLD_PATH = Path(PARQUETS_PATH / "gold" / GOLD_TABLE).resolve().as_posix()
