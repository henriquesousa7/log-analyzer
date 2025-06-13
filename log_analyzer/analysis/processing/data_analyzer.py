from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from log_analyzer.utils import basic_response_size_stats, extract_only_static_resources, extract_weekday_name

class DataAnalyzerHandler:
    """Executa análises específicas sobre o DataFrame de logs."""

    def __init__(self, silver_df: DataFrame, gold_df: DataFrame):
        self.silver_df = silver_df
        self.gold_df = gold_df

    def top_ips(self, n: int = 10) -> DataFrame:
        """Retorna os top N IPs que mais fizeram requisições."""
        return self.silver_df.groupBy("s_client_ip").count().orderBy(F.col("count").desc()).limit(n)

    def top_endpoints(self, n: int = 6) -> DataFrame:
        """
        Retorna os top N endpoints mais acessados,
        ignorando arquivos estáticos (imagens, CSS, JS etc).
        """
        df = extract_only_static_resources(self.silver_df, endpoint_col="s_endpoint")
        return df.groupBy("s_endpoint").count().orderBy(F.col("count").desc()).limit(n)

    def distinct_ip_count(self) -> int:
        """Conta quantos IPs únicos acessaram o sistema."""
        return self.silver_df.select("s_client_ip").distinct().count()

    def unique_day_count(self) -> int:
        """Conta quantos dias distintos existem no dataset."""
        return self.gold_df.select("g_date").distinct().count()

    def response_size_stats(self) -> DataFrame:
        """Calcula estatísticas básicas do tamanho das respostas."""
        return basic_response_size_stats(self.gold_df)

    def most_error_day(self) -> DataFrame:
        """
        Retorna o dia da semana com maior número de erros HTTP.
        Por padrão, analisa erros 4xx (cliente).
        """
        df_with_day = extract_weekday_name(self.gold_df, "g_date")
        
        df_with_day = df_with_day.filter(F.col("g_http_client_errors") > 0)
        return df_with_day.groupBy("weekday").agg(
            F.sum("g_http_client_errors").alias("count")
        ).orderBy(F.col("count").desc()).limit(1)
