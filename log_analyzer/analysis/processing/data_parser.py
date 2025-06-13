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

    def read_bronze_layer(self, input_path: str, output_path: str) -> None:
        """Transforma dados Raw em Bronze"""
        try:
            logging.info(f"Iniciando processamento da bronze")
            sdf = self.input.read_txt(path=input_path)
            bronze_sdf = parse_to_bronze_layer(df=sdf)

            # Aplica o schema explicitamente
            final_bronze_sdf = _apply_explicit_schema(bronze_sdf, DataSchema.bronze_schema())
            
            self.output.save_as_parquet(
                dataframe=final_bronze_sdf,
                output_path=output_path
            )

            logging.info(f"Sucesso no processamento para bronze: no Path {output_path}")
        except Exception as e:
            logging.info(f"Erro no processamento para bronze")
            raise

    def transform_silver_data(self, input_path: str, output_path: str) -> None:
        """Transforma dados Bronze em Silver"""
        try:
            logging.info(f"Iniciando processamento da silver")
            sdf = self.input.read_parquet(path=input_path)
            silver_sdf = parse_to_silver_layer(df=sdf)

            # Validação do schema
            final_silver_df = _apply_explicit_schema(silver_sdf, DataSchema.silver_schema())

            self.output.save_as_parquet(
                dataframe=final_silver_df,
                output_path=output_path,
                partition_by=["s_date"]
            )

            logging.info(f"Sucesso no processamento para silver: no Path {output_path}")
        except Exception as e:
            logging.info(f"Erro no processamento para silver")
            raise

    def acquire_gold_results(self, input_path: str, output_path: str) -> DataFrame:
        """Transforma dados Silver em Gold"""
        try:
            logging.info(f"Iniciando processamento da gold")
            sdf = self.input.read_parquet(path=input_path)
            gold_sdf = parse_to_gold_layer(sdf)

            # Aplica o schema explicitamente
            final_gold_sdf = _apply_explicit_schema(gold_sdf, DataSchema.gold_schema())
            
            self.output.save_as_parquet(
                dataframe=final_gold_sdf,
                output_path=output_path
            )

            logging.info(f"Sucesso no processamento para gold: no Path {output_path}")
        except Exception as e:
            logging.info(f"Erro no processamento para gold")
            raise
    
    