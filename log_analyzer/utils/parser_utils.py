import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def _get_log_regexes():
    """
    Retorna um dicionário com expressões regulares para extrair campos estruturados de linhas de log brutas.

    Campos extraídos:
        - b_client_ip: Endereço IP do cliente
        - b_timestamp: Timestamp da requisição
        - b_method: Método HTTP (GET, POST, etc.)
        - b_endpoint: Recurso requisitado
        - b_protocol: Protocolo utilizado
        - b_status_code: Código de status HTTP
        - b_response_size: Tamanho da resposta em bytes

    Returns:
        dict: Mapeamento de nomes de campos para expressões regulares.
    """
    return {
        # Captura o IP do cliente — é o primeiro conjunto de caracteres não espaços da linha
        "b_client_ip": r'^(\S+)',
        # Captura o timestamp completo entre colchetes, como [03/Dec/2011:13:25:12 -0800]
        # O .*? é "lazy" para parar na primeira ocorrência de ]
        "b_timestamp": r'\[(.*?)\]',
        # Captura o método HTTP dentro das aspas, como GET, POST etc.
        # O \" é a aspa dupla; o (\S+) pega o primeiro elemento dentro das aspas
        "b_method": r'\"(\S+)',
        # Captura o endpoint (URL do recurso requisitado), que é o segundo item dentro das aspas
        # Ignora o método (\S+) e pega o segundo elemento com (\S+)
        "b_endpoint": r'\"(?:\S+)\s(\S+)',
        # Captura o protocolo, como HTTP/1.1, que é o terceiro item dentro das aspas
        # Ignora os dois primeiros (\S+\s\S+) e captura tudo até a próxima aspa
        "b_protocol": r'\"\S+\s\S+\s([^\"]+)',
        # Captura o código de status (200, 404, etc.), que vem logo após a string entre aspas
        # Procura por " seguido de espaço e então 3 dígitos
        "b_status_code": r'\"\s(\d{3})\s',
        # Captura o tamanho da resposta em bytes, que é o último número da linha
        # (\d+) no final da linha com $
        "b_response_size": r'(\d+)$',
    }

def parse_to_bronze_layer(df: DataFrame, column: str = "value") -> DataFrame:
    """
    Extrai informações estruturadas de logs brutos utilizando expressões regulares, criando colunas normalizadas.

    Args:
        df (DataFrame): DataFrame contendo os logs como texto em uma única coluna.
        column (str): Nome da coluna que contém os logs brutos. Padrão: "value".

    Returns:
        DataFrame: DataFrame com colunas separadas para IP, timestamp, método, endpoint, protocolo,
                   código de status e tamanho da resposta.
    """
    regexes = _get_log_regexes()

    for field, pattern in regexes.items():
        df = df.withColumn(field, F.regexp_extract(column, pattern, 1))

    return df.drop(column)

def parse_to_silver_layer(df: DataFrame) -> DataFrame:
    """
    Realiza a transformação da camada Bronze para Silver, incluindo:

    - Conversão do timestamp para os tipos Timestamp e Date.
    - Renomeação de colunas para padrão "s_*".
    - Tratamento de dados nulos ou vazios.
    - Conversão de tipos para análise (status_code e response_size).

    Args:
        df (DataFrame): DataFrame vindo da camada Bronze.

    Returns:
        DataFrame: DataFrame estruturado e pronto para análises na camada Silver.
    """
    # convert timestamp to date
    df = df.withColumn(
        "timestamp_clean",
        F.regexp_replace("b_timestamp", r" -\d{4}$", "")
    ).withColumn(
        "s_datetime",
        F.to_timestamp("timestamp_clean", "dd/MMM/yyyy:HH:mm:ss")
    ).withColumn(
        "s_date",
        F.to_date("s_datetime")
    ).select(
        F.col("b_client_ip").alias("s_client_ip"),
        F.col("b_method").alias("s_method"),
        F.col("b_protocol").alias("s_protocol"),
        F.col("b_endpoint").alias("s_endpoint"),
        F.col("b_status_code").alias("s_status_code"),
        F.col("b_response_size").alias("s_response_size"),
        F.col("s_datetime"),
        F.col("s_date"),
    ).drop("timestamp_clean", "b_timestamp")

    # some accesses don't have a response body, so the response_size column is empty
    df = df.withColumn(
    "s_response_size",
        F.when(F.col("s_response_size") == '', 0)
        .otherwise(F.col("s_response_size").cast("int"))
    )

    # cast status code to int
    df = df.withColumn("s_status_code", F.col("s_status_code").cast("int"))

    return df

def parse_to_gold_layer(df: DataFrame) -> DataFrame:
    """
    Realiza agregações e análises para transformar os dados da camada Silver em informações
    consolidadas na camada Gold. As análises incluem:

    1. IP com mais acessos por data.
    2. Endpoint mais acessado por data.
    3. Número de IPs únicos e estatísticas de volume de dados por data.
    4. Total de erros HTTP do tipo cliente (4xx) por data.

    Args:
        df (DataFrame): DataFrame da camada Silver com logs limpos e tipados.

    Returns:
        DataFrame: DataFrame da camada Gold com colunas agregadas e insights diários.
    """
    # 1. Top Client IP por data
    top_ip = df.groupBy("s_date", "s_client_ip").count().withColumnRenamed(
        "count", "accesses"
    ).withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("s_date").orderBy(F.desc("accesses")))
    ).filter(F.col("rank") == 1).select(
        F.col("s_date").alias("g_date"), F.col("s_client_ip").alias("g_top_client_ip"), F.col("accesses").alias("g_top_client_ip_accesses")
    )

    # 2. Top Endpoint por data
    top_endpoint = df.groupBy(
        "s_date", "s_endpoint"
    ).count().withColumn(
        "rank", F.row_number().over(Window.partitionBy("s_date").orderBy(F.desc("count")))
    ).filter(F.col("rank") == 1).select(
        F.col("s_date").alias("g_date"), F.col("s_endpoint").alias("g_top_endpoint")
    )

    # 3. Distinct IPs por data + Métricas de volume por response_size
    unique_ips = df.groupBy("s_date").agg(
        F.countDistinct("s_client_ip").alias("g_unique_client_ips"),
        F.sum("s_response_size").alias("g_total_volume"),
        F.max("s_response_size").alias("g_max_volume"),
        F.min("s_response_size").alias("g_min_volume"),
        F.avg("s_response_size").alias("g_avg_volume")
    ).withColumnRenamed("s_date", "g_date")

    # 4. Erros HTTP Client Error por data
    http_client_errors = df.filter(
        (F.col("s_status_code") >= 400) & (F.col("s_status_code") < 500)
    ).groupBy("s_date").count().withColumnRenamed("count", "http_client_errors").select(
        F.col("s_date").alias("g_date"), F.col("http_client_errors").alias("g_http_client_errors"),
    )

    # Juntando tudo
    final_df = top_ip \
        .join(top_endpoint, "g_date", "left") \
        .join(unique_ips, "g_date", "left") \
        .join(http_client_errors, "g_date", "left") \
        .fillna({"g_http_client_errors": 0})

    # Mostra o resultado final
    return final_df
