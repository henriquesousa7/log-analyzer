# Imagem base com Spark já configurado
FROM bitnami/spark:latest

# Usa root apenas para instalar dependências
USER root

# Instala dependências do sistema, incluindo postgresql-client (pg_isready)
RUN apt-get update && \
    apt-get install -y unzip && \
    rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho
WORKDIR /app

# Copia todo o código-fonte e dependências para o container
COPY . /app

# Garante que /app esteja no PYTHONPATH para importações internas
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Instala dependências Python
RUN pip install poetry
# RUN pip install --no-cache-dir -r requirements.txt
RUN poetry config virtualenvs.create false \
 && poetry install --no-root --no-interaction --no-cache -v

# Garante permissão de execução ao entrypoint
RUN chmod +x /app/entrypoint.sh

# spark confs
ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_EXECUTOR_MEMORY=4g

EXPOSE 8501
# Define o ponto de entrada
CMD ["/app/entrypoint.sh"]