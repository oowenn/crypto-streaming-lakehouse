#!/usr/bin/env python3
"""
Kraken trades → Kafka (Redpanda) producer using confluent-kafka.

Run:
  python ingestion/kraken_trades_ws.py \
    --pair XBT/USDT \
    --bootstrap localhost:19092 \
    --topic crypto.trades
"""
import argparse, json, time, traceback
from confluent_kafka import Producer
import websocket  # pip install websocket-client

def now_ms() -> int:
    return int(time.time() * 1000)

def parse_args():
    p = argparse.ArgumentParser(description="Kraken trades → Kafka producer")
    p.add_argument("--pair", default="XBT/USDT",
                   help="Kraken pair, e.g. XBT/USDT, ETH/USDT (Kraken uses XBT, not BTC)")
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="crypto.trades")
    p.add_argument("--client-id", default="kraken-trades-producer")
    p.add_argument("--linger-ms", type=int, default=50)
    return p.parse_args()

def make_producer(bootstrap: str, client_id: str, linger_ms: int) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "linger.ms": linger_ms,
        "compression.type": "snappy",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 60000,
    }
    return Producer(conf)

def kraken_url() -> str:
    # Public spot WS v1 (simple trade messages)
    return "wss://ws.kraken.com"

def subscribe_msg(pair: str) -> str:
    msg = {"event": "subscribe", "pair": [pair], "subscription": {"name": "trade"}}
    return json.dumps(msg, separators=(",", ":"))

def normalize_trade(t):
    # Kraken trade entry format (v1 trade channel):
    # [price, volume, time, side, order_type, misc]
    price = float(t[0])
    volume = float(t[1])
    ts_event_ms = int(float(t[2]) * 1000)
    side_raw = t[3]
    side = {"b": "buy", "s": "sell"}.get(side_raw, side_raw)  # handle 'b'/'s' or 'buy'/'sell'
    order_type = {"m": "market", "l": "limit"}.get(t[4], t[4])
    return price, volume, ts_event_ms, side, order_type

def run_once(pair: str, producer: Producer, topic: str):
    url = kraken_url()
    ws = None

    def delivery_report(err, msg):
        if err is not None:
            print(f"[delivery] error for key={msg.key()}: {err}")

    try:
        ws = websocket.create_connection(url, timeout=10)
        ws.send(subscribe_msg(pair))

        while True:
            raw = ws.recv()
            if not raw:
                continue

            try:
                m = json.loads(raw)
            except Exception:
                # forward unknown payloads to Bronze if needed
                continue

            # Event-style messages (subscriptionStatus, heartbeat, systemStatus)
            if isinstance(m, dict):
                # Useful for debugging; ignore here
                # print("[event]", m)
                continue

            # Trade messages come as arrays:
            # [ channel_id, [ [ price, volume, time, side, order_type, misc ], ... ], "trade", "XBT/USDT" ]
            if isinstance(m, list) and len(m) >= 4 and m[2] == "trade":
                trades = m[1]
                pair_out = m[3]
                for t in trades:
                    price, volume, ts_event_ms, side, order_type = normalize_trade(t)

                    out = {
                        "exchange": "kraken",
                        "symbol": pair_out,         # e.g. XBT/USDT
                        "price": price,
                        "size": volume,
                        "side": side,
                        "order_type": order_type,
                        "ts_event": ts_event_ms,    # event time (ms since epoch)
                        "ts_ingest": now_ms(),      # our ingest time
                        # Kraken trade feed v1 doesn't include a trade_id; that's OK for Bronze
                    }

                    key = pair_out.encode("utf-8")
                    val = json.dumps(out, separators=(",", ":")).encode("utf-8")
                    producer.produce(topic=topic, key=key, value=val, on_delivery=delivery_report)
                    producer.poll(0)  # service callbacks

    except KeyboardInterrupt:
        print("Interrupted; closing…")
    except Exception as e:
        print(f"[run_once] error: {e}")
        traceback.print_exc()
    finally:
        try:
            producer.flush(5.0)
        except Exception:
            pass
        if ws is not None:
            try: ws.close()
            except Exception: pass

def main():
    args = parse_args()
    producer = make_producer(args.bootstrap, args.client_id, args.linger_ms)
    print(f"Producing Kraken trades for {args.pair} → '{args.topic}' via {args.bootstrap}")

    backoff = 1.0
    while True:
        try:
            run_once(args.pair, producer, args.topic)
        except KeyboardInterrupt:
            break
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        break

    try:
        producer.flush(5.0)
    except Exception:
        pass

if __name__ == "__main__":
    main()
