import pytest
from pathlib import Path
from log_analyzer.analysis.io import DataOutputHandler, DataInputHandler

def test_save_as_parquet_basic(spark, tmp_path):
    # Cria DataFrame de exemplo
    df = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "letra"])

    # Caminho de saída
    output_path = tmp_path / "parquet_out"

    # Instancia o handler e salva
    handler = DataOutputHandler()
    handler.save_as_parquet(df, str(output_path))

    # Lê novamente para verificar se salvou corretamente
    read_handler = DataInputHandler(spark)
    loaded_df = read_handler.read_parquet(str(output_path)).orderBy("id").collect()

    assert loaded_df[0]["id"] == 1 and loaded_df[0]["letra"] == "X"
    assert loaded_df[1]["id"] == 2 and loaded_df[1]["letra"] == "Y"

def test_save_as_parquet_with_partition(spark, tmp_path):
    # DataFrame com coluna de partição
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "a")], ["id", "part"])

    # Caminho de saída
    output_path = tmp_path / "parquet_partitioned"

    # Salva com particionamento
    handler = DataOutputHandler()
    handler.save_as_parquet(df, str(output_path), partition_by="part")

    # Verifica se as pastas de partição foram criadas
    partition_folders = [p.name for p in output_path.iterdir() if p.is_dir()]
    assert "part=a" in partition_folders
    assert "part=b" in partition_folders