import os
import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from log_analyzer.analysis.io import DataInputHandler, DataOutputHandler
from log_analyzer.analysis.models import DataSchema
from log_analyzer.utils import parse_to_bronze_layer, parse_to_silver_layer, parse_to_gold_layer

def _apply_explicit_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Aplica explicitamente o schema a um DataFrame, com cast e alias.

    Args:
        df (DataFrame): DataFrame de entrada.
        schema (StructType): Schema alvo (StructType) com os nomes e tipos desejados.

    Returns:
        DataFrame com as colunas convertidas para os tipos definidos no schema.
    """
    return df.select(
        *[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in schema.fields
        ]
    )

class DataParserHandler:
    """Responsável pela transformação dos dados em todas as camadas"""
    def __init__(self, input: DataInputHandler, output: DataOutputHandler):
        self.input = input
        self.output = output

    def read_bronze_layer(self, input_path: os.PathLike, output_path: os.PathLike) -> None:
        """Transforma dados Raw em Bronze"""
        try:
            logging.info("Iniciando processamento da Bronze")
            sdf = self.input.read_txt(path=input_path)

            logging.info("Iniciando a transformação dos dados para a camada Bronze")
            bronze_sdf = parse_to_bronze_layer(df=sdf)

            # Aplica o schema explicitamente
            final_bronze_sdf = _apply_explicit_schema(bronze_sdf, DataSchema.bronze_schema())

            self.output.save_as_parquet(
                dataframe=final_bronze_sdf,
                output_path=output_path
            )

            logging.info(f"Sucesso na transformação para Bronze: no Path {output_path}")
        except Exception:
            logging.info("Erro no processamento para Bronze")
            raise

    def transform_silver_data(self, input_path: os.PathLike, output_path: os.PathLike) -> None:
        """Transforma dados Bronze em Silver"""
        try:
            logging.info("Iniciando processamento da Silver")
            sdf = self.input.read_parquet(path=input_path)

            logging.info("Iniciando a transformação dos dados para a camada Silver")
            silver_sdf = parse_to_silver_layer(df=sdf)

            # Validação do schema
            final_silver_df = _apply_explicit_schema(silver_sdf, DataSchema.silver_schema())

            self.output.save_as_parquet(
                dataframe=final_silver_df,
                output_path=output_path,
                partition_by=["s_date"]
            )

            logging.info(f"Sucesso na transformação para Silver: no Path {output_path}")
        except Exception:
            logging.info("Erro no processamento para Silver")
            raise

    def acquire_gold_results(self, input_path: os.PathLike, output_path: os.PathLike) -> DataFrame:
        """Transforma dados Silver em Gold"""
        try:
            logging.info("Iniciando processamento da Gold")
            sdf = self.input.read_parquet(path=input_path)

            logging.info("Iniciando a transformação dos dados para a camada Gold")
            gold_sdf = parse_to_gold_layer(sdf)

            # Aplica o schema explicitamente
            final_gold_sdf = _apply_explicit_schema(gold_sdf, DataSchema.gold_schema())

            self.output.save_as_parquet(
                dataframe=final_gold_sdf,
                output_path=output_path,
                partition_by=["g_date"]
            )

            logging.info(f"Sucesso na transformação para Gold: no Path {output_path}")
        except Exception:
            logging.info("Erro no processamento para Gold")
            raise

