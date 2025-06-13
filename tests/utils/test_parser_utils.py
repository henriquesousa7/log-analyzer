import pytest
from log_analyzer.utils import (
    parse_to_bronze_layer,
    parse_to_silver_layer,
    parse_to_gold_layer,
)
from log_analyzer.utils.parser_utils import _get_log_regexes

def test_get_log_regexes():
    regexes = _get_log_regexes()
    expected_keys = {
        "b_client_ip",
        "b_timestamp",
        "b_method",
        "b_endpoint",
        "b_protocol",
        "b_status_code",
        "b_response_size"
    }
    assert set(regexes.keys()) == expected_keys
    for pattern in regexes.values():
        assert isinstance(pattern, str)

def test_parse_to_bronze_layer(spark):
    log_line = '192.168.0.1 - - [01/Jan/2025:10:00:00 -0300] "GET /home HTTP/1.1" 200 1024'
    df = spark.createDataFrame([(log_line,)], ["value"])

    result = parse_to_bronze_layer(df)

    row = result.first().asDict()
    assert row["b_client_ip"] == "192.168.0.1"
    assert row["b_timestamp"] == "01/Jan/2025:10:00:00 -0300"
    assert row["b_method"] == "GET"
    assert row["b_endpoint"] == "/home"
    assert row["b_protocol"] == "HTTP/1.1"
    assert row["b_status_code"] == "200"
    assert row["b_response_size"] == "1024"

def test_parse_to_silver_layer(spark):
    bronze_data = [
        (
            "192.168.0.1",
            "01/Jan/2025:10:00:00 -0300",
            "GET",
            "/home",
            "HTTP/1.1",
            "200",
            "1024"
        )
    ]
    df = spark.createDataFrame(bronze_data, [
        "b_client_ip", "b_timestamp", "b_method", "b_endpoint",
        "b_protocol", "b_status_code", "b_response_size"
    ])

    result = parse_to_silver_layer(df).first().asDict()

    assert result["s_client_ip"] == "192.168.0.1"
    assert result["s_method"] == "GET"
    assert result["s_endpoint"] == "/home"
    assert result["s_status_code"] == 200
    assert result["s_response_size"] == 1024
    assert result["s_datetime"] is not None
    assert result["s_date"] is not None

def test_parse_to_gold_layer(spark):
    from datetime import datetime

    silver_data = [
        (
            "192.168.0.1", "GET", "HTTP/1.1", "/home", 200, 1024,
            datetime(2025, 1, 1, 10, 0, 0), datetime(2025, 1, 1).date()
        ),
        (
            "192.168.0.2", "POST", "HTTP/1.1", "/login", 404, 512,
            datetime(2025, 1, 1, 11, 0, 0), datetime(2025, 1, 1).date()
        ),
        (
            "192.168.0.1", "GET", "HTTP/1.1", "/home", 200, 256,
            datetime(2025, 1, 1, 12, 0, 0), datetime(2025, 1, 1).date()
        ),
    ]
    df = spark.createDataFrame(silver_data, [
        "s_client_ip", "s_method", "s_protocol", "s_endpoint",
        "s_status_code", "s_response_size", "s_datetime", "s_date"
    ])

    result = parse_to_gold_layer(df).first().asDict()

    assert result["g_date"] == datetime(2025, 1, 1).date()
    assert result["g_top_client_ip"] == "192.168.0.1"
    assert result["g_top_client_ip_accesses"] == 2
    assert result["g_top_endpoint"] == "/home"
    assert result["g_unique_client_ips"] == 2
    assert result["g_total_volume"] == 1792
    assert result["g_http_client_errors"] == 1