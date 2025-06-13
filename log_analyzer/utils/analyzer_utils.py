import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def extract_only_static_resources(df: DataFrame, endpoint_col: str = "endpoint"):
    """
    Retorna uma expressão booleana para identificar endpoints de arquivos estáticos.
    """
    # Remove query string e aplica filtro
    df = df.withColumn("path_only", F.split(F.col(endpoint_col), r"\?").getItem(0))

    return df.filter(~F.col("path_only").rlike(r"\.\w{2,5}$")).drop("path_only")

def extract_weekday_name(df: DataFrame, date_col: str, alias: str = "weekday") -> DataFrame:
    """
    Adiciona uma coluna com o nome do dia da semana (ex: Monday, Tuesday).

    :param df: DataFrame original
    :param date_col: Nome da coluna de data
    :param alias: Nome da nova coluna com o nome do dia da semana
    :return: Novo DataFrame com a coluna adicionada
    """
    return df.withColumn(alias, F.date_format(date_col, "EEEE"))

def basic_response_size_stats(df: DataFrame, col_name: str = "g_total_volume") -> DataFrame:
    """
    Retorna estatísticas básicas da coluna response_size.

    :param df: DataFrame de entrada
    :param col_name: Nome da coluna com os tamanhos das respostas
    :return: DataFrame com colunas: total, max, min, avg
    """
    return df.select(
        F.sum(col_name).alias("total_volume"),
        F.max(col_name).alias("max_volume"),
        F.min(col_name).alias("min_volume"),
        F.avg(col_name).alias("avg_volume")
    )