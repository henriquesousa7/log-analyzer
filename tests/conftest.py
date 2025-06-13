import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("WebLogTest") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def logs_text():
    log_data = [
        '10.64.224.191 - - [03/Dec/2011:13:25:12 -0800] "GET /images/filmpics/0000/5649/MVM_Black_Belt_v6_thumb.jpg HTTP/1.1" 200 74716',
        '10.124.155.234 - - [03/Dec/2011:13:26:03 -0800] "GET /release-schedule HTTP/1.1" 200 3602',
        '10.64.224.191 - - [03/Dec/2011:13:26:07 -0800] "GET /displaytitle.php?id=274 HTTP/1.1" 200 2905'
    ]
    return log_data