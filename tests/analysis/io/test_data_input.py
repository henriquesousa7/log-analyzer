import pytest
from pathlib import Path
from log_analyzer.analysis.io import DataInputHandler

def test_read_txt(spark, tmp_path):
    # Cria arquivo de texto temporário
    txt_path = tmp_path / "example.txt"
    txt_path.write_text("linha 1\nlinha 2")

    handler = DataInputHandler(spark)
    df = handler.read_txt(str(txt_path))

    # Testa se o conteúdo foi lido corretamente
    lines = [row.value for row in df.collect()]
    assert lines == ["linha 1", "linha 2"]

def test_read_parquet(spark, tmp_path):
    # Cria DataFrame de exemplo e salva como Parquet
    df_original = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "letra"])
    parquet_dir = tmp_path / "parquet_dir"
    df_original.write.parquet(str(parquet_dir))

    handler = DataInputHandler(spark)
    df_loaded = handler.read_parquet(str(parquet_dir))

    # Verifica se os dados lidos são os mesmos
    data = df_loaded.orderBy("id").collect()
    assert data[0]["id"] == 1 and data[0]["letra"] == "A"
    assert data[1]["id"] == 2 and data[1]["letra"] == "B"