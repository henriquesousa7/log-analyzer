import json
import streamlit as st
from pyspark.sql import SparkSession
from log_analyzer.config import SILVER_PATH, GOLD_PATH
from log_analyzer.analysis.io import DataInputHandler
from log_analyzer.analysis.processing import DataAnalyzerHandler

def _get_spark_session() -> SparkSession:
    """Creates and configures a Spark session."""
    spark = (
        SparkSession.builder
        .appName("WebLogAnalysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Instantiate components
spark = _get_spark_session()
input = DataInputHandler(spark=spark)


# Get data and instantiate analyzer
silver_df = input.read_parquet(path=SILVER_PATH)
gold_df = input.read_parquet(path=GOLD_PATH)

analyzer = DataAnalyzerHandler(silver_df=silver_df, gold_df=gold_df)

st.set_page_config(page_title="Dashboard de Logs")
st.title("Dashboard de Análise de Logs")

# --- Top IPs ---
st.header("Top 10 IPs com mais acessos")
st.dataframe(analyzer.top_ips().toPandas())
#st.bar_chart(analyzer.top_ips().set_index("s_client_ip")["count"])

# --- Top endpoints ---
st.header("Top 6 Endpoints (sem arquivos)")
st.dataframe(analyzer.top_endpoints().toPandas())
#st.bar_chart(analyzer.top_endpoints().set_index("s_endpoint")["count"])

# --- IPs e dias únicos ---
st.header("Informações gerais")
col1, col2 = st.columns(2)
col1.metric("IPs distintos", f'{analyzer.distinct_ip_count():,}')
col2.metric("Dias únicos", f'{analyzer.unique_day_count():,}')

# --- Estatísticas de tamanho de resposta ---
st.header("Estatísticas de tamanho de resposta")
st.dataframe(analyzer.response_size_stats().toPandas())

# --- Dia com mais erros ---
st.header("Dia com mais erros 4xx")
st.dataframe(analyzer.most_error_day().toPandas())
#st.bar_chart(analyzer.most_error_day().set_index("weekday")["count"])
