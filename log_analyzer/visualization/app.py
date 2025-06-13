import streamlit as st
from pyspark.sql import SparkSession, DataFrame
from log_analyzer.config import SILVER_PATH, GOLD_PATH
from log_analyzer.analysis.io import DataInputHandler
from log_analyzer.analysis.processing import DataAnalyzerHandler

st.set_page_config(page_title="Dashboard de Logs")
@st.cache_resource
def _get_spark_session() -> SparkSession:
    """Cria e retorna uma SparkSession com nível de log reduzido."""
    spark = (
        SparkSession.builder
        .appName("WebLogAnalysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def _load_dataframes(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Carrega os DataFrames das camadas Silver e Gold."""
    input_handler = DataInputHandler(spark=spark)
    silver_df = input_handler.read_parquet(path=SILVER_PATH).cache()
    gold_df = input_handler.read_parquet(path=GOLD_PATH).cache()
    
    # force cache to next steps
    silver_df.count(); gold_df.count()
    return silver_df, gold_df

def render_dashboard(analyzer: DataAnalyzerHandler) -> None:
    """Renderiza os componentes do dashboard no Streamlit."""
    st.title("Dashboard de Análise de Logs")

    # --- Top IPs ---
    st.header("Top 10 IPs com mais acessos")
    st.dataframe(analyzer.top_ips().toPandas())

    # --- Top endpoints ---
    st.header("Top 6 Endpoints (sem arquivos)")
    st.dataframe(analyzer.top_endpoints().toPandas())

    # --- IPs e dias únicos ---
    st.header("Informações gerais")
    col1, col2 = st.columns(2)
    col1.metric("IPs distintos", f"{analyzer.distinct_ip_count():,}")
    col2.metric("Dias únicos", f"{analyzer.unique_day_count():,}")

    # --- Estatísticas de tamanho de resposta ---
    st.header("Estatísticas de tamanho de resposta")
    st.dataframe(analyzer.response_size_stats().toPandas())

    # --- Dia com mais erros ---
    st.header("Dia com mais erros 4xx")
    st.dataframe(analyzer.most_error_day().toPandas())


def main():
    try:
        spark = _get_spark_session()
        with st.spinner("Rodando a etapa de Load e Análise de dados."):
            silver_df, gold_df = _load_dataframes(spark)
            analyzer = DataAnalyzerHandler(silver_df=silver_df, gold_df=gold_df)

            render_dashboard(analyzer)
    except Exception as e:
        st.error(f"Erro ao carregar o dashboard: {str(e)}")


if __name__ == "__main__":
    main()
