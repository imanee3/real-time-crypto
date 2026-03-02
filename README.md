#  Real-Time Crypto Intelligence Pipeline

> Pipeline de données temps réel pour l'investissement en cryptomonnaies  
> **DATA NEXT** × **INVISTIS** — Février 2026

---

## Vue d'ensemble

Ce pipeline ingère des données crypto en temps réel depuis **Binance WebSocket**, **NewsAPI** et **FRED API**, les traite via **Spark Structured Streaming sur Databricks**, et les écrit vers un **Delta Lake** et **Supabase PostgreSQL**.

```
Binance WS ──┐
NewsAPI ──────┼──► Confluent Kafka ──► Spark Databricks ──► Delta Lake
                                          ──► Supabase
                                                         
```

---

### Flux de données

1. **Ingestion** : Producers Python → Kafka topics (Confluent Cloud)
2. **Processing** : Spark Structured Streaming (Databricks Serverless)
   - Parse JSON + Schema Validation
   - Stream-to-Stream Join avec Watermark 5min
   - Windowing 1min / 5min
   - Enrichissement FRED (macro data)
   - Scoring sentiment
3. **Fan-Out** : Multi-sink via `foreachBatch`
   - → Delta Lake (raw + enriched)
   - → Supabase REST API (warehouse)
   - → alerts_topic (Kafka)

---

## Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Message Broker | Confluent Cloud (Kafka) | Latest |
| Stream Processing | Databricks Serverless | Spark 3.5 |
| Data Lake | Delta Lake | /Volumes Unity Catalog |
| Warehouse | Supabase (PostgreSQL) | 15 |
| Orchestration | Apache Airflow | 2.x |
| Language | Python | 3.10+ |
| Serialization | JSON / Parquet | — |

---

## Sources de données

### 1. Binance WebSocket
- **Endpoint** : `wss://stream.binance.com:9443/ws/btcusdt@trade`
- **Données** : symbol, price, quantity, timestamp, trade_id
- **Fréquence** : Temps réel (tick-by-tick)

### 2. NewsAPI
- **Endpoint** : `https://newsapi.org/v2/everything?q=bitcoin`
- **Données** : title, description, source, published_at, sentiment
- **Fréquence** : Toutes les heures

---

## Kafka Topics

| Topic | Description | Schéma |
|-------|-------------|--------|
| `trades_topic` | Trades Binance temps réel | `{symbol, price, quantity, timestamp, trade_id}` |
| `news_topic` | Articles NewsAPI | `{title, description, source, published_at, sentiment}` |

### Configuration Confluent Cloud

```python
kafka_options = {
    "kafka.bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="API_KEY" password="API_SECRET";',
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}
```

---

## Spark Structured Streaming

### Stream-to-Stream Join

```python
# Watermark obligatoire sur les deux streams
trades_df = trades_df.withWatermark("event_time", "5 minutes")
news_df   = news_df.withWatermark("event_time", "5 minutes")

# Join dans une fenêtre ±5 minutes
joined_df = trades_df.alias("t").join(
    news_df.alias("n"),
    expr("""
        t.symbol = n.symbol AND
        t.event_time BETWEEN n.event_time - INTERVAL 5 MINUTES
                         AND n.event_time + INTERVAL 5 MINUTES
    """),
    "inner"
)
```

### Batch Join (implémenté en production)

```python
# Fenêtre 24h pour maximiser les correspondances trades/news
joined_batch = trades_batch.alias("t").join(
    news_batch.alias("n"),
    (col("t.symbol") == col("n.symbol")) &
    (col("t.event_time").between(
        col("n.event_time") - expr("INTERVAL 24 HOURS"),
        col("n.event_time") + expr("INTERVAL 24 HOURS")
    )),
    "left"
)
```

### Multi-Sink Fan-Out

```python
# Sink 1 — Delta Lake
joined_batch.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/invistis/datalake/raw/enriched")

# Sink 2 — Supabase REST API
response = requests.post(
    "https://XXXX.supabase.co/rest/v1/enriched_trades",
    headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
    data=json.dumps(batch)
)
```

---

## Outputs

### Supabase — Table `enriched_trades`

```sql
CREATE TABLE enriched_trades (
    symbol       TEXT,
    price        FLOAT8,
    quantity     FLOAT8,
    trade_time   TIMESTAMP,
    news_title   TEXT,
    sentiment    FLOAT8,
    news_time    TIMESTAMP,
    window_start TIMESTAMP,
    window_end   TIMESTAMP
);
```

> **3 993 lignes ingérées** — Février 2026

---

## Installation & Configuration

### Prérequis

```bash
pip install kafka-python confluent-kafka pyspark requests psycopg2-binary pandas numpy
```

### Variables d'environnement

```bash
# Confluent Kafka
CONFLUENT_BOOTSTRAP="pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY="YOUR_API_KEY"
CONFLUENT_SECRET="YOUR_API_SECRET"

# Supabase
SUPABASE_URL="https://XXXX.supabase.co"
SUPABASE_KEY="YOUR_ANON_KEY"

# NewsAPI
NEWS_API_KEY="YOUR_NEWS_KEY"
```

### Databricks — Checkpoints

> **Important** : Les checkpoints Spark Streaming **doivent** être sur `/Volumes` (Unity Catalog).  
> Le path `/Workspace` est interdit pour le state store RocksDB en mode Serverless.

```python
CHECKPOINT_PATH = "/Volumes/invistis/datalake/raw/checkpoints"
DATA_PATH       = "/Volumes/invistis/datalake/raw/enriched"
```

---

## Structure du projet

```
│   ├── binance_producer.py        # WebSocket → trades_topic
│   ├── news_producer.py           # NewsAPI → news_topic
│   └── spark_streaming_pipeline   # Databricks notebook principal
├── supabase/
│   └── schema.sql                 # DDL table enriched_trades
└── README.md
```

---
 join conservé en mode batch
