import pytest
from pyspark.sql import Row
from log_analyzer.utils import (
    extract_only_static_resources,
    extract_weekday_name,
    basic_response_size_stats,
)

def test_extract_only_static_resources(spark):
    data = [
        Row(endpoint="/index.html"),
        Row(endpoint="/api/data"),
        Row(endpoint="/style.css"),
        Row(endpoint="/dashboard"),
        Row(endpoint="/image.png?version=1"),
    ]
    df = spark.createDataFrame(data)
    
    result = extract_only_static_resources(df, "endpoint")
    output = [row["endpoint"] for row in result.collect()]

    assert "/api/data" in output
    assert "/dashboard" in output
    assert "/index.html" not in output
    assert "/style.css" not in output
    assert "/image.png?version=1" not in output


def test_extract_weekday_name(spark):
    from datetime import datetime

    data = [("2025-06-09",), ("2025-06-10",)]  # Monday, Tuesday
    df = spark.createDataFrame(data, ["date_str"])
    df = df.withColumn("date", df["date_str"].cast("timestamp"))

    result = extract_weekday_name(df, "date", alias="weekday")
    weekdays = [row["weekday"] for row in result.select("weekday").collect()]

    assert weekdays == ["Monday", "Tuesday"]


def test_basic_response_size_stats(spark):
    data = [(100,), (200,), (50,)]
    df = spark.createDataFrame(data, ["g_total_volume"])

    result = basic_response_size_stats(df).collect()[0]

    assert result["total_volume"] == 350
    assert result["max_volume"] == 200
    assert result["min_volume"] == 50
    assert result["avg_volume"] == pytest.approx(116.66, rel=1e-2)