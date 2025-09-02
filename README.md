# Crypto Streaming Lakehouse

Live crypto trades → Kafka (Redpanda) → **Bronze** (raw audit) → **Silver** (parsed & typed) → **Gold** (1-minute OHLCV + VWAP).

This is a local, laptop-friendly stack using Docker + Python + PySpark. Your `notebook.ipynb` contains verification cells for each layer.

---

## Stack (dev)
- **Broker:** Redpanda (Kafka API) + Console UI (http://localhost:8080)
- **Ingestion:** `ingestion/kraken_trades_ws.py` (Kraken public WS → Kafka)
- **Processing:** Spark Structured Streaming (PySpark)
- **Storage:** Parquet on local disk

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
pip install -r requirements.txt            # confluent-kafka + websocket-client
pip install "pyspark==3.5.1"
```

### 2) Ingest trades → Kafka (Kraken)
```bash
python ingestion/kraken_trades_ws.py   --pair XBT/USDT   --bootstrap localhost:19092   --topic crypto.trades
```
Sanity:
```bash
docker exec -it redpanda rpk topic consume crypto.trades -n 5
```

### 3) Kafka → **Bronze** (raw audit)
Writes raw JSON + Kafka metadata to Parquet.
```bash
python spark/jobs/kafka_to_bronze_trades.py   --bootstrap localhost:19092   --topic crypto.trades   --out data/bronze/crypto_trades   --checkpoint data/checkpoints/kafka_to_bronze_trades   --startingOffsets earliest   --trigger "2 seconds"
```

### 4) Bronze → **Silver** (parsed, typed, event-time, dedup)
```bash
python spark/jobs/bronze_to_silver_trades.py   --bronze data/bronze/crypto_trades   --silver data/silver/trades   --checkpoint data/checkpoints/bronze_to_silver_trades
```

### 5) Silver → **Gold** (1-minute OHLCV + VWAP)
```bash
python spark/jobs/silver_to_gold_bars.py   --silver data/silver/trades   --gold data/gold/bars_1m   --checkpoint data/checkpoints/silver_to_gold_bars_1m
```

---

## Verifying results (use `notebook.ipynb`)
- **Bronze:** reads `data/bronze/crypto_trades`, prints row count & latest `offset`s.
- **Silver:** reads `data/silver/trades`, shows `symbol, price, size, side, event_time, ingest_time`.
- **Gold:** reads `data/gold/bars_1m`, shows `symbol, bar_start, open, high, low, close, volume, vwap, trades` ordered by most recent.

---

## Model notes
- **Bronze**: immutable raw (`value_raw`) + Kafka metadata (`topic, partition, offset, ts_kafka`). No parsing.
- **Silver**: parsed columns, `price/size` numeric, `event_time/ingest_time` TIMESTAMP, light dedup on `symbol,event_time,price,size,side`.
- **Gold (1m)**: event-time windows per symbol → `open, high, low, close, volume, vwap, trades`. Partitioned by day & symbol.

---

## Operations
- Stop Spark jobs: **Ctrl-C** in the job terminal.
- Safe restarts thanks to **checkpointLocation**.
- Dev reset (clean slate):
```bash
rm -rf data/checkpoints/kafka_to_bronze_trades        data/checkpoints/bronze_to_silver_trades        data/checkpoints/silver_to_gold_bars_1m        data/bronze/crypto_trades        data/silver/trades        data/gold/bars_1m
```

---

## Troubleshooting
- **Broker/UI points to :9092** → use `localhost:19092` from host, `redpanda:29092` from containers.
- **Producer `_ALL_BROKERS_DOWN`** → broker not up or wrong bootstrap.
- **Spark `Failed to find data source: kafka`** → include Kafka package (already set in Bronze job via `spark.jars.packages`).
- **Bronze has files but 0 rows** → keep producer running; delete that job’s checkpoint + output; restart with `--startingOffsets earliest`.
- **Gold job “Schema must be specified”** → the script infers Silver schema first and passes it to `readStream`.

---

## Roadmap (next)
- 5-minute & 1-hour bars; volatility & returns
- Basic dashboard (Superset/Grafana) + simple alerts
- Optional: multi-pair ingestion & symbol normalization
