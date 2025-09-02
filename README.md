# Crypto Streaming Lakehouse

Real-time crypto ETL with Kafka-API (**Redpanda**) + Spark (Flink optional).  
We ingest exchange trades, land **Bronze → Silver → Gold** tables, and serve live dashboards/alerts.

## What’s in here (so far)
- **Infra:** Local Redpanda broker + Console UI via Docker Compose.
- **Next steps:** Ingestion (websocket → Kafka), then Spark jobs (Bronze→Silver, Silver→Gold).

## Medallion layers (quick)
- **Bronze**: raw JSON from exchange feeds (immutable).
- **Silver**: parsed, typed, deduped tables (trades/order books).
- **Gold**: analytics (OHLCV, VWAP, spreads, realized vol) for dashboards/alerts.

---

## Quickstart

### 1) Prerequisites
- Docker Desktop installed and running.

### 2) Start local broker + UI
```bash
docker compose -f infra/docker-compose.yml up -d
```

### 3) Connection addresses
- From **your laptop/host**: `localhost:19092`
- From **other Docker containers**: `redpanda:29092`
- Console UI: http://localhost:8080

> We use **dual listeners** so host apps and containers both work.

---

## Sanity check

### A) Cluster info
```bash
docker exec -it redpanda rpk cluster info
```

### B) Create a topic
```bash
docker exec -it redpanda rpk topic create crypto.trades
```

### C) Produce a test message (pick one)

**Option 1 — heredoc (no TTY):**
```bash
docker exec -i redpanda rpk topic produce crypto.trades <<'EOF'
{"symbol":"BTCUSDT","price":63542.12,"size":0.001,"ts":1693587432123}
EOF
```

**Option 2 — pipe:**
```bash
printf '%s\n' '{"symbol":"BTCUSDT","price":63542.12,"size":0.001,"ts":1693587432123}' | docker exec -i redpanda rpk topic produce crypto.trades
```

**Option 3 — interactive:**
```bash
docker exec -it redpanda rpk topic produce crypto.trades
# paste one JSON line, then press Ctrl-D
```

### D) Consume it back
```bash
docker exec -it redpanda rpk topic consume crypto.trades -n 1
```

You should see the JSON payload echoed from the topic.

---

## Repo layout
```
infra/
  docker-compose.yml      # Redpanda + Console (dual listeners)
ingestion/                # (next) websocket → Kafka producers
spark/
  jobs/                   # (later) Bronze→Silver, Silver→Gold streaming jobs
configs/
notebooks/
```

---

## Milestones
- **M1:** Binance trades → Kafka (Bronze files for audit)
- **M2:** Bronze→Silver (schema, types, dedup, watermark)
- **M3:** Silver→Gold (1-min OHLCV, VWAP, spreads, realized vol)
- **M4:** Dashboard + simple alerts (Superset/Grafana + webhooks)
- **M5 (opt):** Flink parity for Silver→Gold to compare latency/semantics

---

## Troubleshooting

**Console shows `localhost:9092` / connection refused**  
The broker was advertising a host-only address. We use **dual listeners**:
- INTERNAL: `redpanda:29092` (for containers)
- EXTERNAL: `localhost:19092` (for host)

Restart compose after edits:
```bash
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up -d --force-recreate
```

**Port already in use (9092)**  
Another Kafka is running. Either stop it or use the provided `19092` host port.

**First seconds show `connection refused`**  
Broker might still be starting. Retry after a moment.

**Stop everything**
```bash
docker compose -f infra/docker-compose.yml down
```

---

## Next up
Step 3: **Minimal ingestion** — Binance websocket → produce trades to `crypto.trades`, with a tiny config and retries. Then we’ll add the Spark Bronze→Silver job.

## License
MIT
