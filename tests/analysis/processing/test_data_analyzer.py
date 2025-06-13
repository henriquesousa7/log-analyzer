import pytest
from datetime import datetime
from log_analyzer.analysis.processing import DataAnalyzerHandler
from pyspark.sql import Row

@pytest.fixture
def silver_df(spark):
    data = [
        Row(s_client_ip="10.0.0.1", s_endpoint="/home"),
        Row(s_client_ip="10.0.0.1", s_endpoint="/style.css"),
        Row(s_client_ip="10.0.0.2", s_endpoint="/home"),
        Row(s_client_ip="10.0.0.3", s_endpoint="/home"),
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def gold_df(spark):
    data = [
        Row(g_date=datetime(2024, 6, 10), g_response_size=1000, g_http_client_errors=1),
        Row(g_date=datetime(2024, 6, 11), g_response_size=2000, g_http_client_errors=2),
        Row(g_date=datetime(2024, 6, 12), g_response_size=500,  g_http_client_errors=0),
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def analyzer(silver_df, gold_df):
    return DataAnalyzerHandler(silver_df, gold_df)

def test_top_ips(analyzer):
    result = analyzer.top_ips(2).collect()
    assert result[0]["s_client_ip"] == "10.0.0.1"
    assert result[0]["count"] == 2

def test_top_endpoints(analyzer):
    result = analyzer.top_endpoints(2).collect()
    endpoints = [row["s_endpoint"] for row in result]
    assert "/home" in endpoints
    assert all(not ep.endswith((".css", ".js", ".jpg")) for ep in endpoints)

def test_distinct_ip_count(analyzer):
    assert analyzer.distinct_ip_count() == 3

def test_unique_day_count(analyzer):
    assert analyzer.unique_day_count() == 3

def test_most_error_day(analyzer):
    result = analyzer.most_error_day().collect()
    assert result[0]["weekday"] == "Tuesday"
    assert result[0]["count"] == 2