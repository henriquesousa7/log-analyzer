import os
import logging
from pyspark.sql import DataFrame

class DataInputHandler:
    """Gerencia a leitura nas camadas Bronze, Silver e Gold"""
    
    def __init__(self, spark):
        """
        Inicializa o gerenciador do data lake
        
        Args:
            spark: Sessao Spark
        """
        self.spark = spark

    def read_txt(self, path: os.PathLike):
        """
        Método unificado para leitura de dados de Texto

        Args:
            path (str): Caminho para o arquivo Texto.

        Returns:
            DataFrame: Spark DataFrame com os dados lidos
        """
        logging.info(f"Lendo arquivo de {path}")
        return self.spark.read.text(path)
    
    def read_parquet(
        self,
        path: os.PathLike,
        format: str = "parquet",
        **read_options
    ) -> DataFrame:
        """
        Método unificado para leitura de dados Parquet
        
        Args:
            path (str): Caminho para o arquivo Parquet.
            format (str): Formato de leitura dos dados. Valor padrão é 'parquet'.
            read_options: Opções adicionais para o Spark DataFrameReader.
            
        Returns:
            DataFrame: DataFrame com os dados lidos
        """
        logging.info(f"Lendo Parquet de {path}")
        return self.spark.read.format(format).options(**read_options).load(path)