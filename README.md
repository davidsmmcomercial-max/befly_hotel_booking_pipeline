# BeFly Hotel Booking Pipeline

Pipeline de dados em **PySpark** utilizando arquitetura **Medalhão** — Bronze, Silver e Gold — para processamento e análise de reservas de hotéis.

O projeto simula um pipeline local de Data Lake para responder perguntas de negócio sobre receita, ocupação, cancelamentos e perfil dos hóspedes.

---

## Tecnologias

- Python
- PySpark
- Parquet
- Google Colab
- Git/GitHub
- Arquitetura Medalhão

---

## Estrutura do Projeto

```text
befly_hotel_booking_pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── notebooks/
│   └── befly_hotel_booking_pipeline.ipynb
│
├── scripts/
│   ├── raw_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
└── data/
    ├── raw/
    │   ├── hotel_bookings.csv
    │   ├── country_metadata.csv
    │   └── hotel_metadata.csv
    │
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## Dataset

O dataset principal utilizado é o **Hotel Booking Demand**.

Fonte pública:

- Kaggle: https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand

Arquivos esperados:

```text
hotel_bookings.csv
country_metadata.csv
hotel_metadata.csv
```

Os arquivos devem estar disponíveis em:

```text
data/raw/
```

---

## Camadas do Pipeline

### Bronze

A camada Bronze realiza a ingestão dos arquivos CSV em formato Parquet, preservando os dados o mais próximo possível da origem.

Saídas:

```text
data/bronze/bookings/
data/bronze/countries/
data/bronze/hotels/
```

A tabela `bookings` é particionada por:

```text
arrival_date_year
```

---

### Silver

A camada Silver aplica limpeza, padronização e enriquecimento dos dados.

Principais transformações:

- Conversão segura de tipos com `try_cast`
- Conversão de `reservation_status_date` para `date`
- Criação da coluna `arrival_date`
- Tratamento de `children` nulo como `0`
- Tratamento de `country` nulo como `UNK`
- Remoção de reservas sem hóspedes
- Remoção de registros com `adr < 0`
- Criação de colunas derivadas:
  - `total_nights`
  - `total_guests`
  - `is_family`
  - `is_long_stay`
  - `revenue`
  - `booking_status`
- Enriquecimento com:
  - `country_metadata`
  - `hotel_metadata`

Saída:

```text
data/silver/bookings_enriched/
```

Particionamento:

```text
arrival_date_year
arrival_date_month_num
```

---

### Gold

A camada Gold gera visões analíticas agregadas para consumo por BI/negócio.

Tabelas geradas:

```text
data/gold/revenue_by_hotel_month/
data/gold/cancellation_by_segment/
data/gold/top_countries_by_revenue/
data/gold/guest_stay_profile/
```

#### 1. revenue_by_hotel_month

Responde:

> Quanto cada hotel fatura mês a mês?

Métricas:

- `total_bookings`
- `effective_bookings`
- `cancelled_bookings`
- `total_revenue_eur`
- `avg_adr_eur`
- `total_nights_sold`
- `cancellation_rate_pct`

#### 2. cancellation_by_segment

Responde:

> Quais segmentos e canais apresentam maior taxa de cancelamento?

Métricas:

- `total_bookings`
- `cancelled_bookings`
- `cancellation_rate_pct`
- `avg_lead_time`
- `avg_total_special_requests`

#### 3. top_countries_by_revenue

Responde:

> De quais países vêm os hóspedes mais valiosos?

Métricas:

- `effective_bookings`
- `total_revenue_eur`
- `avg_ticket_eur`
- `avg_lead_time`

#### 4. guest_stay_profile

Responde:

> Qual o perfil médio de estadia e hóspedes por hotel e tipo de cliente?

Métricas:

- `avg_total_nights`
- `avg_total_guests`
- `pct_long_stay`
- `pct_family`

---

## Como rodar localmente

### 1. Clonar o repositório

```bash
git clone https://github.com/davidsmmcomercial-max/befly_hotel_booking_pipeline.git
cd befly_hotel_booking_pipeline
```

---

### 2. Criar ambiente virtual

#### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### Linux/Mac

```bash
python -m venv .venv
source .venv/bin/activate
```

---

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

---

### 4. Baixar os arquivos CSV

Baixe os arquivos do dataset e coloque em:

```text
data/raw/
```

A estrutura esperada é:

```text
data/raw/hotel_bookings.csv
data/raw/country_metadata.csv
data/raw/hotel_metadata.csv
```

---

### 5. Executar o pipeline

Execute os scripts utilizando Python:

```bash
python scripts/raw_to_bronze.py
python scripts/bronze_to_silver.py
python scripts/silver_to_gold.py
```

Ou utilizando Spark Submit:

```bash
spark-submit scripts/raw_to_bronze.py
spark-submit scripts/bronze_to_silver.py
spark-submit scripts/silver_to_gold.py
```
### 6. Validar as saídas

Após a execução, as camadas serão geradas em:

```text
data/bronze/
data/silver/
data/gold/
```

---

## Como rodar no Google Colab

### 1. Abrir o notebook

Abra o arquivo:

```text
notebooks/befly_hotel_booking_pipeline.ipynb
```

no Google Colab.

---

### 2. Instalar dependências

Execute a primeira célula do notebook:

```python
!pip install -q pyspark
```

---

### 3. Baixar os CSVs do GitHub

Os arquivos CSV devem ser baixados a partir da pasta:

```text
data/raw/
```

disponível neste repositório.

Arquivos necessários:

```text
hotel_bookings.csv
country_metadata.csv
hotel_metadata.csv
```

---

### 4. Fazer upload dos CSVs no Colab

No menu lateral esquerdo do Colab:

```text
Files > Upload
```

Envie os três arquivos CSV.

---

### 5. Criar a estrutura de pastas no Colab

O notebook cria a estrutura:

```text
data/raw/
data/bronze/
data/silver/
data/gold/
```

Depois move os arquivos CSV para:

```text
data/raw/
```

---

### 6. Executar as células em ordem

Execute o notebook de cima para baixo:

1. Instalação e criação da SparkSession
2. Criação das pastas
3. Leitura dos CSVs
4. Camada Bronze
5. Camada Silver
6. Camada Gold
7. Validações finais

---

### Observação sobre o Colab

O ambiente do Google Colab é temporário.  
Ao encerrar a sessão, os arquivos locais podem ser apagados.

Por isso, para reproduzir a execução:

1. Abra o notebook
2. Faça upload dos CSVs novamente
3. Rode as células em ordem

---

## Data Quality

Durante a Silver foram aplicadas validações de qualidade:

```text
Registros antes dos filtros: 119390
Removidos sem hóspedes: 180
Removidos com ADR negativo: 1
Registros finais Silver: 119209
```

Também foi validado que os joins com tabelas de referência não duplicaram registros.

---

## Arquitetura em Produção

Este pipeline poderia ser adaptado para um ambiente cloud utilizando uma arquitetura Lakehouse na AWS.

Uma possível arquitetura seria:

```text
Amazon S3
  bronze/
  silver/
  gold/
```

Serviços sugeridos:

- **Amazon S3** para armazenamento das camadas do Data Lake
- **AWS Glue Jobs** para execução PySpark
- **AWS Glue Data Catalog** para catálogo de tabelas
- **Amazon Athena** para consultas analíticas
- **Apache Iceberg** para versionamento, schema evolution e operações ACID
- **AWS Step Functions** ou **Apache Airflow** para orquestração
- **AWS Lake Formation** para governança e controle de acesso
- **Amazon CloudWatch** para logs, métricas e observabilidade

Em produção, as tabelas Silver e Gold poderiam ser particionadas por ano e mês, reduzindo custo de leitura e melhorando performance em consultas temporais.

---

## Considerações

O dataset foi processado integralmente, sem recorte de volume.

A solução prioriza:

- clareza de código
- separação lógica por camadas
- rastreabilidade
- qualidade de dados
- organização para consumo analítico
- facilidade de adaptação para cloud
