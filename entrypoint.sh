#!/bin/bash
set -e

# run tests
echo "[entrypoint] Rodando testes com pytest..."
python -m pytest

# echo "Testes concluídos com sucesso."

# unzip log.zip
echo "Descompactando arquivos..."
unzip -j -o data/access_log.zip -d data/
echo "Arquivo descompactado"

echo "Iniciando processamento de logs..."

# ------------------------------------------------------------------------------
# spark-submit: ferramenta oficial do Apache Spark para executar jobs.
# --master local[*] : roda o Spark em modo local, usando todos os núcleos da máquina.
# /app/app/main.py  : caminho do script principal da aplicação, dentro do contêiner.
# ------------------------------------------------------------------------------
/opt/bitnami/spark/bin/spark-submit --master local[*] /app/scripts/main.py

echo "Processamento finalizado com sucesso."

# echo "Iniciando Streamlit..."
# Rodar Streamlit (supondo que seu app está em /app/web/app.py)
python -m streamlit run /app/log_analyzer/visualization/app.py --server.port 8501