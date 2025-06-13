import os
import logging
from typing import List, Optional, Union

class DataOutputHandler:
    """
    Gerencia operações de persistência de dados em diferentes formatos de arquivo.
    Fornece métodos para armazenar dados em formatos binários otimizados.

    Operações suportadas:
        - save_as_parquet: Armazena um DataFrame no formato Parquet com opções personalizáveis.
    """

    def save_as_parquet(
        self, dataframe,
        output_path: os.PathLike,
        mode="overwrite",
        partition_by: Optional[Union[str, List[str]]] = None,
        merge_schema: bool = False,
        format: str = "parquet"
    ):
        """
         Salva um DataFrame no formato Parquet com configurações otimizadas

        Parameters:
            dataframe: DataFrame Spark a ser salvo.
            output_path: Caminho completo de destino.
            mode: Modo de escrita (overwrite, append, ignore, error).
            partition_by: Coluna(s) para particionamento.
            merge_schema: Se True, mescla schemas durante escrita incremental.
            format (str): Formato de save dos dados. Valor padrão é 'parquet'.
        """
        try:
            self._prepare_output_directory(output_path)

            # Configura o writer base
            writer = dataframe.write.format(format).mode(mode.lower())

             # Opções condicionais
            if merge_schema:
                writer.option("mergeSchema", "true")

            # Particionamento se especificado
            if partition_by:
                if isinstance(partition_by, str):
                    partition_by = [partition_by]

                writer.partitionBy(*partition_by)

            # Executa a escrita
            writer.save(output_path)
            logging.info(f"Parquet data successfully persisted to: {output_path}")
        except Exception as error:
            logging.error(f"Failed to write to {output_path}. Error: {str(error)}")
            raise

    def _prepare_output_directory(self, path: os.PathLike):
        """Ensures the target directory structure exists"""
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
            logging.debug(f"Created directory structure: {dir_path}")
