# BeFly Hotel Booking Pipeline

Pipeline de dados em PySpark utilizando arquitetura Medalhão (Bronze, Silver e Gold).

## Tecnologias
- Python
- PySpark
- Google Colab
- Parquet
- Arquitetura Lakehouse

## Estrutura

data/
  bronze/
  silver/
  gold/

scripts/
  befly_hotel_booking_pipeline.py

## Camadas

### Bronze
Ingestão dos CSVs originais em Parquet.

### Silver
- Limpeza
- Tratamento de nulos
- Conversão de tipos
- Criação de métricas derivadas
- Enriquecimento com tabelas de referência

### Gold
Visões analíticas:
- Receita por hotel/mês
- Cancelamentos por segmento
- Top países por receita

## Como executar

Instalar dependências:

pip install pyspark

Executar notebook no Google Colab ou scripts localmente.

## Arquitetura Cloud

O pipeline pode ser adaptado para AWS utilizando:
- S3 como Data Lake
- AWS Glue para processamento
- Athena para consumo analítico
- Airflow/EventBridge para orquestração
- Lake Formation para governança
