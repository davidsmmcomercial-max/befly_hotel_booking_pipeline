# BeFly Hotel Booking Pipeline

Pipeline de dados em PySpark utilizando arquitetura Medalhão (Bronze, Silver e Gold).

## Objetivo

Este projeto simula um pipeline de engenharia de dados para processamento de reservas de hotéis utilizando:

- PySpark
- Arquitetura Medalhão
- Parquet + Snappy
- Docker
- DataFrames API

O pipeline foi construído com foco em:

- Ingestão de dados brutos
- Limpeza e enriquecimento
- Criação de visões analíticas
- Organização em camadas Bronze, Silver e Gold

---

# Arquitetura Medalhão

## Bronze

Responsável por armazenar os dados brutos em formato Parquet.

### Entradas

- hotel\_bookings.csv
- country\_metadata.csv
- hotel\_metadata.csv

### Saídas

```text
/data/bronze/bookings/
/data/bronze/countries/
/data/bronze/hotels/
```

---

## Silver

Responsável pela limpeza, padronização e enriquecimento.

### Transformações realizadas

- Conversão de tipos
- Tratamento de nulos
- Criação de colunas derivadas
- Conversão de datas
- Enriquecimento com tabelas de referência
- Validação de joins
- Remoção de registros inválidos

### Colunas derivadas

- total\_nights
- total\_guests
- is\_family
- is\_long\_stay
- revenue
- booking\_status
- arrival\_date

### Saída

```text
/data/silver/bookings_enriched/
```

---

## Gold

Responsável pelas visões analíticas.

### Visões criadas

#### revenue\_by\_hotel\_month

Pergunta de negócio:

> Quanto cada hotel faturou por mês?

Métricas:

- total\_bookings
- effective\_bookings
- cancelled\_bookings
- total\_revenue
- avg\_adr
- total\_nights\_sold
- cancellation\_rate

---

#### cancellation\_by\_segment

Pergunta de negócio:

> Quais segmentos cancelam mais reservas?

Métricas:

- total\_bookings
- cancelled\_bookings
- cancellation\_rate
- avg\_lead\_time
- avg\_total\_special\_requests

---

#### top\_countries\_by\_revenue

Pergunta de negócio:

> Quais países geram maior receita?

Métricas:

- effective\_bookings
- total\_revenue
- avg\_ticket
- avg\_lead\_time

---

#### guest\_profile\_analysis

Pergunta de negócio:

> Como é o perfil médio dos hóspedes?

Métricas:

- avg\_total\_nights
- avg\_total\_guests
- pct\_long\_stay
- pct\_family

---

# Estrutura do Projeto

```text
befly_hotel_booking_pipeline/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│   └── befly_hotel_booking_pipeline.ipynb
│
├── scripts/
│   ├── raw_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
└── README.md
```

---

# Tecnologias Utilizadas

- Python 3.11
- PySpark 3.5.1
- Apache Spark
- Docker
- Parquet
- Snappy
- Google Colab

---

# Execução via Docker (Recomendado)

A execução via Docker é a forma recomendada para evitar problemas de compatibilidade do PySpark/Hadoop no Windows.

## Pré-requisitos

- Docker Desktop instalado
- Docker Compose habilitado

Verifique:

```bash
docker --version
docker compose version
```

---

# 1. Clonar o repositório

```bash
git clone https://github.com/davidsmmcomercial-max/befly_hotel_booking_pipeline.git
cd befly_hotel_booking_pipeline
```

---

# 2. Adicionar os datasets

Baixe os arquivos CSV e coloque em:

```text
data/raw/
```

Arquivos esperados:

```text
data/raw/hotel_bookings.csv
data/raw/country_metadata.csv
data/raw/hotel_metadata.csv
```

---

# 3. Build da imagem Docker

```bash
docker compose build --no-cache
```

---

# 4. Executar o container

```bash
docker compose run --rm befly-pipeline bash
```

Você entrará no ambiente Linux do container:

```bash
root@container:/app#
```

---

# 5. Executar o pipeline

Execute os scripts na ordem:

```bash
python scripts/raw_to_bronze.py
```

```bash
python scripts/bronze_to_silver.py
```

```bash
python scripts/silver_to_gold.py
```

---

# 6. Estrutura gerada

Após execução:

```text
data/
├── bronze/
├── silver/
└── gold/
```

---

# Execução no Google Colab

O notebook também pode ser executado no Google Colab.

## Passos

### 1. Abrir o notebook

Abra:

```text
notebooks/befly_hotel_booking_pipeline.ipynb
```

---

### 2. Fazer upload dos datasets

Baixe os arquivos CSV disponíveis em:

```text
data/raw/
```

E faça upload manual para o Colab.

Arquivos necessários:

```text
hotel_bookings.csv
country_metadata.csv
hotel_metadata.csv
```

---

### 3. Executar as células

Execute as células sequencialmente.

O notebook:

- cria as camadas Bronze
- cria a Silver
- cria a Gold
- mostra amostras analíticas
- grava os resultados em Parquet

---

# Observações Importantes

## Compatibilidade Windows

A execução local do PySpark no Windows pode gerar erros relacionados ao Hadoop/WinUtils:

```text
UnsatisfiedLinkError
NativeIO$Windows.access0
```

Por esse motivo, o projeto utiliza Docker como abordagem recomendada para garantir:

- compatibilidade multiplataforma
- reprodutibilidade
- ambiente isolado

---

# Decisões Técnicas

## Formato Parquet

Utilizado devido:

- compressão eficiente
- leitura colunar
- melhor performance analítica
- padrão amplamente usado em Data Lakes

---

## Particionamento

### Bronze

Particionado por:

```text
arrival_date_year
```

### Silver

Particionado por:

```text
arrival_date_year
arrival_date_month_num
```

Isso melhora queries temporais.

---

## Qualidade dos Dados

Validações implementadas:

- remoção de reservas sem hóspedes
- remoção de ADR negativo
- tratamento de nulos
- validação de joins
- checagem de duplicidade após enriquecimento

---

# Arquitetura em Produção (AWS)

Em ambiente produtivo, este pipeline poderia ser adaptado para:

- Amazon S3 como Data Lake
- AWS Glue Jobs para processamento Spark
- AWS Glue Catalog para metadados
- Athena para consultas analíticas
- Airflow ou Step Functions para orquestração
- Lake Formation para governança
- Apache Iceberg ou Delta Lake para tabelas ACID
- CI/CD via GitHub Actions ou GitLab CI

## Estrutura sugerida no S3

```text
s3://befly-data-lake/
├── bronze/
├── silver/
└── gold/
```

---

# Dataset Utilizado

Dataset público:

Hotel Booking Demand Dataset

Fonte:

[https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand)

---

# Autor

David M

