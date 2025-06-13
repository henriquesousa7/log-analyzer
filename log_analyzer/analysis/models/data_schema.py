from pyspark.sql.types import *

class DataSchema:
    """Define os schemas para cada camada"""
    
    @staticmethod
    def bronze_schema():
        return StructType([
            StructField("b_client_ip", StringType(), False),
            StructField("b_timestamp", StringType(), False),
            StructField("b_method", StringType(), False),
            StructField("b_endpoint", StringType(), False),
            StructField("b_protocol", StringType(), False),
            StructField("b_status_code", StringType(), False),
            StructField("b_response_size", StringType(), False),
        ])
    
    @staticmethod
    def silver_schema():
        return StructType([
            StructField("s_client_ip", StringType(), False),
            StructField("s_method", StringType(), False),
            StructField("s_endpoint", StringType(), False),
            StructField("s_protocol", StringType(), False),
            StructField("s_status_code", IntegerType(), False),
            StructField("s_response_size", IntegerType(), False),
            StructField("s_datetime", TimestampType(), False),
            StructField("s_date", DateType(), False),
        ])


    @staticmethod
    def gold_schema():
        return StructType([
            StructField("g_date", DateType(), False),
            StructField("g_top_client_ip", StringType(), False),
            StructField("g_top_client_ip_accesses", IntegerType(), False),
            StructField("g_top_endpoint", StringType(), False),
            StructField("g_unique_client_ips", IntegerType(), False),
            StructField("g_total_volume", LongType(), False),
            StructField("g_max_volume", LongType(), False),
            StructField("g_min_volume", LongType(), False),
            StructField("g_avg_volume", LongType(), False),
            StructField("g_http_client_errors", IntegerType(), False),
        ])