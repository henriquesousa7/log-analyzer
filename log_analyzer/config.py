import os

# Tabelas
BRONZE_TABLE = os.getenv("BRONZE_TABLE")
SILVER_TABLE = os.getenv("SILVER_TABLE")
GOLD_TABLE = os.getenv("GOLD_TABLE")

# Parquet and Log Path
PARQUETS_PATH = os.getenv("PARQUETS_PATH")
LOG_PATH = os.getenv("LOG_PATH")

# Caminhos com nomes das tabelas incluídos
BRONZE_PATH = os.path.abspath(os.getenv("BRONZE_PATH", os.path.join(PARQUETS_PATH, "bronze", BRONZE_TABLE)))
SILVER_PATH = os.path.abspath(os.getenv("SILVER_PATH", os.path.join(PARQUETS_PATH, "silver", SILVER_TABLE)))
GOLD_PATH = os.path.abspath(os.getenv("GOLD_PATH", os.path.join(PARQUETS_PATH, "gold", GOLD_TABLE)))
