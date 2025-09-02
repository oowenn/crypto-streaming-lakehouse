# Crypto Streaming Lakehouse

Live crypto trades → Kafka (Redpanda) → **Bronze** (raw audit) → **Silver** (parsed & typed).  
This repo is runnable on a laptop with Docker + Python.

---

## Stack (dev)
- **Broker:** Redpanda (Kafka API) + Console UI (http://localhost:8080)
- **Ingestion:** `ingestion/kraken_trades_ws.py` (Kraken public WS → Kafka)
- **Storage:** Parquet on local disk
- **Processing:** Spark Structured Streaming (PySpark)

**Dual listeners (important):**
- Host apps → `localhost:19092`
- Containers → `redpanda:29092`

---

## Quickstart

### 0) Start broker + UI
```bash
docker compose -f infra/docker-compose.yml up -d
# UI: http://localhost:8080
```

### 1) Python env & deps
```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt          # confluent-kafka + websocket-client
pip install "pyspark==3.5.1"
```

### 2) Ingest trades → Kafka (Kraken)
```bash
python ingestion/kraken_trades_ws.py   --pair XBT/USDT   --bootstrap localhost:19092   --topic crypto.trades
```
Sanity check in another terminal:
```bash
docker exec -it redpanda rpk topic consume crypto.trades -n 5
```

### 3) Kafka → **Bronze** (raw audit)
Writes raw JSON + Kafka metadata to Parquet.
```bash
python spark/jobs/kafka_to_bronze_trades.py   --bootstrap localhost:19092   --topic crypto.trades   --out data/bronze/crypto_trades   --checkpoint data/checkpoints/kafka_to_bronze_trades   --startingOffsets earliest   --trigger "2 seconds"
```
Verify Bronze step in (`notebook.ipynb`)

### 4) Bronze → **Silver** (parsed, typed, deduped, event-time)
Parses `value_raw`, casts numeric fields, converts timestamps, and drops simple duplicates.
```bash
python spark/jobs/bronze_to_silver_trades.py   --bronze data/bronze/crypto_trades   --silver data/silver/trades   --checkpoint data/checkpoints/bronze_to_silver_trades
```
Verify Silver step in (`notebook.ipynb`)

---

## Model notes
- **Bronze**: immutable raw (`value_raw`) + Kafka metadata (`topic, partition, offset, ts_kafka`). No parsing.
- **Silver**: parsed columns, `price/size` as numbers, `event_time/ingest_time` as TIMESTAMP, simple dedup (`symbol,event_time,price,size,side`).
- Use **event-time** for windows later; measure latency with `ingest_time`/`ts_kafka`.

---

## Operations
- Stop Spark jobs: **Ctrl-C** in the job terminal.
- Safe restarts thanks to **checkpointLocation**.
- Dev reset (clean slate):
```bash
rm -rf data/checkpoints/kafka_to_bronze_trades        data/checkpoints/bronze_to_silver_trades        data/bronze/crypto_trades        data/silver/trades
```

---

## Troubleshooting
- **Broker issues / UI shows localhost:9092:** Use the correct listener (`localhost:19092` on host).
- **Producer `_ALL_BROKERS_DOWN`:** Broker not running or wrong bootstrap.
- **Spark `Failed to find data source: kafka`:** Include the Kafka package (already set in the Bronze job via `spark.jars.packages`). If using `spark-submit`, add:  
  `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
- **Bronze files present but 0 rows:** Keep producer running, delete that job’s checkpoint + output, restart with `--startingOffsets earliest` and a short `--trigger`.
- **Nulls after parsing in Silver:** Adjust `payload_schema` to match actual JSON. Inspect a sample `value_raw` from Bronze.

---

## Repo layout
```
infra/
  docker-compose.yml
ingestion/
  kraken_trades_ws.py
spark/
  jobs/
    kafka_to_bronze_trades.py
    bronze_to_silver_trades.py
data/                 # (gitignored runtime data)
  bronze/
  silver/
  checkpoints/
README.md
requirements.txt
```

---

## Roadmap (next)
- **Gold**: 1‑min OHLCV + VWAP (event-time windows + watermark)
- Optional: cross‑exchange symbol normalization, dashboard/alerts

---

## License
MIT
