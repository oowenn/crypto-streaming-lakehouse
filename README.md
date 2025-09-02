# Crypto Streaming Lakehouse

> **Scope:** This README documents the stack **up to Bronze** (landing raw data). Silver/Gold will be added later.

We ingest live trades from **Kraken's public WebSocket**, publish to **Redpanda** (Kafka API), and land an **append-only Bronze audit trail** with **Spark Structured Streaming**.

---

## Architecture (current)

```
Kraken WebSocket
    └── ingestion/kraken_trades_ws.py  (confluent-kafka Producer)
          └── topic: crypto.trades  (Redpanda, Kafka API)
                └── Spark job: kafka_to_bronze_trades.py
                      └── Parquet files: data/bronze/crypto_trades/
                             + _spark_metadata
```

- **Message key** = trading pair (e.g., `XBT/USDT`) → keeps per-pair order in one partition.
- **Bronze** = raw JSON bytes (`value_raw`) + Kafka metadata (`topic`, `partition`, `offset`, `ts_kafka`).

---

## Prerequisites

- Docker Desktop running
- Python 3.10+ (works on 3.13)
- `pip` and `venv`

---

## 0) Start broker + UI

We use **dual Kafka listeners** so host apps and containers both work:

- Host → `localhost:19092`
- Containers → `redpanda:29092`
- Console UI → <http://localhost:8080>

Start:
```bash
docker compose -f infra/docker-compose.yml up -d
```

Optional sanity:
```bash
docker exec -it redpanda rpk cluster info
# (topic auto-create is enabled; creating explicitly is optional)
docker exec -it redpanda rpk topic create crypto.trades || true
```

---

## 1) Python env & dependencies

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt          # confluent-kafka + websocket-client
pip install "pyspark==3.5.1"
```

`requirements.txt` (reference):
```
confluent-kafka>=2.6,<3
websocket-client==1.8.0
```

---

## 2) Ingest trades → Kafka (Kraken)

```bash
python ingestion/kraken_trades_ws.py   --pair XBT/USDT   --bootstrap localhost:19092   --topic crypto.trades
```
Quick check (new terminal):
```bash
docker exec -it redpanda rpk topic consume crypto.trades -n 5
```

You should see JSON values with fields like `symbol`, `price`, `size`, `side`, `ts_event`, `ts_ingest`.

---

## 3) Kafka → **Bronze** (raw audit trail)

This Structured Streaming job writes raw Kafka records to Parquet.

Run:
```bash
python spark/jobs/kafka_to_bronze_trades.py   --bootstrap localhost:19092   --topic crypto.trades   --out data/bronze/crypto_trades   --checkpoint data/checkpoints/kafka_to_bronze_trades   --startingOffsets earliest   --trigger "2 seconds"
```

What it writes (columns):
- `topic`, `partition`, `offset`, `ts_kafka`, `ts_type`
- `key` (string), `value_raw` (raw JSON as string)

**Why Bronze stays raw:** It’s an immutable ledger for **audit & reprocessing**. Parsing/typing happens in **Silver** (later).

---

## 4) Verify Bronze files

In a Python REPL / notebook:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("data/bronze/crypto_trades")
print("rows:", df.count())
df.orderBy("offset", ascending=False).show(10, False)
df.printSchema()
```
Tip to see which files have rows:
```python
from pyspark.sql.functions import input_file_name
df.groupBy(input_file_name().alias("file")).count().orderBy("count").show(20, False)
```

---

## Operations

**Stop jobs:** press **Ctrl-C** in that terminal.  
**Restart safety:** because we set a **checkpoint**, Spark resumes from the last committed offsets.

**Dev reset (only if you want a clean slate):**
```bash
rm -rf data/checkpoints/kafka_to_bronze_trades
rm -rf data/bronze/crypto_trades
```
Then re-run with `--startingOffsets earliest` while your producer is sending data.

---

## Troubleshooting (Bronze-specific)

**Files appear empty / 0 rows**  
- Producer wasn’t sending yet → keep producer running, delete checkpoint + output (dev only), restart with `--startingOffsets earliest` and a short `--trigger`.
- Confirm input with: `docker exec -it redpanda rpk topic consume crypto.trades -n 3`.

**Spark error: `Failed to find data source: kafka`**  
- Make sure the Bronze job sets `spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
  (already configured in the script). If you use `spark-submit`, pass `--packages` explicitly.

**Producer `_ALL_BROKERS_DOWN`**  
- Wrong bootstrap or broker isn’t up. Use `localhost:19092`, check `docker ps`.

---

## Repo layout (current)

```
infra/
  docker-compose.yml            # Redpanda + Console
ingestion/
  kraken_trades_ws.py           # Kraken WS → Kafka producer (confluent-kafka)
spark/
  jobs/
    kafka_to_bronze_trades.py   # Kafka → Bronze Parquet (raw + metadata)
data/                            # (gitignored runtime data)
  bronze/
  checkpoints/
README.md
requirements.txt
```

---

## Roadmap (next steps, not included here)

- Bronze → **Silver**: parse JSON, cast types, convert timestamps, dedup with watermarks.
- Silver → **Gold**: 1-min OHLCV & VWAP (event-time windows).

---

## License
MIT
