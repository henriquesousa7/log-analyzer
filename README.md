# Log Analyzer
> Last Version: __1.0.0__

## Análise de Logs Web com Apache Spark

Este projeto tem como objetivo processar e analisar logs de acesso a um servidor web utilizando **Apache Spark**, a fim de extrair informações relevantes sobre o comportamento dos usuários e o desempenho da infraestrutura.  

A solução é empacotada via **Docker** e **Docker Compose**, com os resultados persistidos em **Parquet** para  serem visualizados via **Streamlit**.

---

## Desafio Proposto

A análise dos logs responde às seguintes questões:

1. **Top 10 IPs:** Quais são os 10 IPs de origem com maior número de acessos?
2. **Top 6 Endpoints:** Quais são os 6 endpoints mais acessados, desconsiderando arquivos estáticos como `.js`, `.css`, `.png`, etc.?
3. **IPs Distintos:** Quantos IPs únicos acessaram o servidor?
4. **Dias Únicos:** Quantos dias diferentes estão representados nos logs?
5. **Estatísticas de Tamanho das Respostas:**
   - Volume total de dados retornados.
   - Maior volume de dados em uma única resposta.
   - Menor volume de dados em uma única resposta.
   - Volume médio de dados retornado.
6. **Dia com Mais Erros 4xx:** Qual dia da semana teve mais respostas com erros do tipo "HTTP Client Error" (códigos 4xx)?

Os resultados são persistidos em **Parquet**, utilizando a Arquitetura de medalhão.

---

## Requisitos e Instalação

### Pré-requisitos

Certifique-se de ter as seguintes ferramentas instaladas:

- **Docker Desktop** (Windows/macOS): [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
- **Docker Engine** e **Docker Compose** (Linux):  
  - [Instalar Docker Engine](https://docs.docker.com/engine/install/)  
  - [Instalar Docker Compose](https://docs.docker.com/compose/install/)

### Clonando o Repositório

```bash
git clone https://github.com/henriquesousa7/web-log-analysis.git
cd web-log-analysis
```

---

## Como Executar o Projeto

### 1. Construir as Imagens Docker

No diretório raiz (onde está o `docker-compose.yml`), execute:

```bash
docker-compose build --no-cache
```

> O uso do `--no-cache` é recomendado na primeira execução ou após alterações em dependências ou no Dockerfile.

### 2. Iniciar os Serviços

Execute os containers com:

```bash
docker-compose up
```

> Os serviços incluem:
> - **log-analyzer**: processa os logs e gera os arquivos do tipo Parquet de saída.

Para rodar em segundo plano:

```bash
docker-compose up -d
```

### 3. Verificar os Resultados

- **Parquets Gerados:** Localizados em `./data_outputs` (mapeado de `/app/data_outputs` no container).
- **Logs:** Serão exibidos no terminal ou podem ser acessados via `docker logs`.

### 4. Encerrar os Serviços

```bash
docker-compose down
```

Para remover também os volumes persistentes (incluindo banco e saída de dados):

```bash
docker-compose down -v
```

---

## Visualização de Dados com Streamlit

Após os steps de processamento e ingestão, uma aplicação **Streamlit** é iniciada para gerar dashboards interativos baseados no arquivo **JSON** exportado.  

A interface facilita a exploração dos dados de forma visual e acessível.

🔗 Acesse a aplicação:  
```
http://localhost:8501/
```

---

## Justificativa da Persistência

Optou-se pelo uso do formato **Parquet** por ser colunar, compacto e altamente eficiente para análises com o Apache Spark. Essa escolha facilita futuras consultas e integrações com ferramentas analíticas, otimizando a leitura e o armazenamento dos dados processados a partir dos logs (Volume alto de dados, sendo baseado por n users e n timestamps).

---