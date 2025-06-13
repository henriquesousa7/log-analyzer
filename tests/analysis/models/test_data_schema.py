import pytest
from pyspark.sql.types import *
from log_analyzer.analysis.models import DataSchema

def test_bronze_schema_fields():
    schema = DataSchema.bronze_schema()
    expected_fields = [
        ("b_client_ip", StringType(), False),
        ("b_timestamp", StringType(), False),
        ("b_method", StringType(), False),
        ("b_endpoint", StringType(), False),
        ("b_protocol", StringType(), False),
        ("b_status_code", StringType(), False),
        ("b_response_size", StringType(), False),
    ]
    assert schema.fields == [StructField(*f) for f in expected_fields]


def test_silver_schema_fields():
    schema = DataSchema.silver_schema()
    expected_fields = [
        ("s_client_ip", StringType(), False),
        ("s_method", StringType(), False),
        ("s_endpoint", StringType(), False),
        ("s_protocol", StringType(), False),
        ("s_status_code", IntegerType(), False),
        ("s_response_size", IntegerType(), False),
        ("s_datetime", TimestampType(), False),
        ("s_date", DateType(), False),
    ]
    assert schema.fields == [StructField(*f) for f in expected_fields]


def test_gold_schema_fields():
    schema = DataSchema.gold_schema()
    expected_fields = [
        ("g_date", DateType(), False),
        ("g_top_client_ip", StringType(), False),
        ("g_top_client_ip_accesses", IntegerType(), False),
        ("g_top_endpoint", StringType(), False),
        ("g_unique_client_ips", IntegerType(), False),
        ("g_total_volume", LongType(), False),
        ("g_max_volume", LongType(), False),
        ("g_min_volume", LongType(), False),
        ("g_avg_volume", LongType(), False),
        ("g_http_client_errors", IntegerType(), False),
    ]
    assert schema.fields == [StructField(*f) for f in expected_fields]