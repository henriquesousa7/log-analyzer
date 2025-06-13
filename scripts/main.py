import logging

from pyspark.sql import SparkSession
from log_analyzer.analysis.io import DataInputHandler, DataOutputHandler
from log_analyzer.analysis.processing import DataParserHandler
from log_analyzer.config import LOG_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def _get_spark_session() -> SparkSession:
    """Creates and configures a Spark session."""
    spark = (
        SparkSession.builder
        .appName("WebLogAnalysis")
        .getOrCreate()
    )

    # INFO: Define o nível de log da aplicação Spark como "ERROR",
    # ocultando mensagens de nível "INFO" e "WARN" que geralmente poluem o console.
    # Isso ajuda a focar apenas em mensagens críticas de erro durante a execução.
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def _run_pipeline(spark: SparkSession):
    try:
        logging.info("Pipeline iniciado")
        input = DataInputHandler(spark)
        output = DataOutputHandler()

        parser = DataParserHandler(
            input=input,
            output=output
        )

        # Run step-by-step
        parser.read_bronze_layer(input_path=LOG_PATH, output_path=BRONZE_PATH)
        parser.transform_silver_data(input_path=BRONZE_PATH, output_path=SILVER_PATH)
        parser.acquire_gold_results(input_path=SILVER_PATH, output_path=GOLD_PATH)
        logging.info("Pipeline finalizado")
    except Exception as e:
        logging.error(f"Error: {e}")
        raise


def main():
    try:
        spark = _get_spark_session()
        # Exec pipeline
        _run_pipeline(spark)
    except Exception as e:
        logging.error(f"Falha no pipeline: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session encerrada.")


if __name__ == "__main__":
    main()
