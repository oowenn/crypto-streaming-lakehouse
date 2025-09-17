# Crypto Streaming Lakehouse

Live crypto trades → Kafka (Redpanda) → **Bronze** (raw audit) → **Silver** (parsed & typed) → **Gold** (1-minute OHLCV + VWAP).

This is a local, laptop-friendly stack using Docker + Python + PySpark. `notebooks/read_data.ipynb` contains verification cells for each layer.

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

## Usage

### 1) Container Setup

#### Commands
```bash
# start containers, wait for broker, create topic, and print bootstrap
scripts/infra.sh up

# check status / topics
scripts/infra.sh status

# stream broker logs
scripts/infra.sh logs

# tear down everything (containers + volumes)
scripts/infra.sh down
```

#### Env Overrides
```bash
COMPOSE_FILE=infra/docker-compose.yml   # path to compose file
BROKER_NAME=redpanda                    # service/container name for broker
CONSOLE_NAME=redpanda-console           # service/container name for console
BROKER_INTERNAL_PORT=9092/tcp           # container port to resolve host port
TOPIC=crypto.trades PARTITIONS=1 REPLICAS=1
WAIT_SECS=60                            # broker readiness timeout (seconds)
```

#### Examples
```bash
$ TOPIC=my.trades PARTITIONS=3 scripts/infra.sh up
$ COMPOSE_FILE=infra/alt.yml scripts/infra.sh up
$ scripts/infra.sh status
$ scripts/infra.sh down
```

### 2) Data Collection

#### Commands
```bash
./collect_data.sh start-all           # start: ingestion, bronze, silver, gold (background)
./collect_data.sh stop-all            # stop all four
./collect_data.sh status              # show which are running
./collect_data.sh tail                # tail all logs

# Start/stop one service:
./collect_data.sh start ingestion
./collect_data.sh start bronze
./collect_data.sh start silver
./collect_data.sh start gold

./collect_data.sh stop ingestion|bronze|silver|gold
```

#### Env Overrides
```bash
PAIR="XBT/USDT"             # ingestion pair (Kraken)
TOPIC="crypto.trades"
BOOTSTRAP="localhost:19092"

BRONZE_DIR="data/bronze/crypto_trades"
SILVER_DIR="data/silver/trades"
GOLD_DIR="data/gold/bars_1m"
CHECKPOINT_DIR="data/checkpoints"
```

#### Examples
```bash
$ PAIR=ETH/USDT TOPIC=my.trades ./collect_data.sh start-all
$ BOOTSTRAP=localhost:9092 ./collect_data.sh start bronze
$ ./collect_data.sh tail
$ ./collect_data.sh stop-all && ./collect_data.sh status
```

### 2) Monitor Size

#### Commands
```bash
scripts/count_rows.sh        # text output
JSON=1 scripts/count_rows.sh # JSON output
```

#### Env Overrides
```bash
BRONZE=/full/path/to/data/bronze/crypto_trades
SILVER=/full/path/to/data/silver/trades
GOLD=/full/path/to/data/gold/bars_1m
JSON=0|1
```

#### Examples
```bash
$ scripts/count_rows.sh
$ JSON=1 scripts/count_rows.sh
```

---

## Model notes
- **Bronze**: immutable raw (`value_raw`) + Kafka metadata (`topic, partition, offset, ts_kafka`). No parsing.
- **Silver**: parsed columns, `price/size` numeric, `event_time/ingest_time` TIMESTAMP, light dedup on `symbol,event_time,price,size,side`.
- **Gold (1m)**: event-time windows per symbol → `open, high, low, close, volume, vwap, trades`. Partitioned by day & symbol.