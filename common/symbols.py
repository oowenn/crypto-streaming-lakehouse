# common/symbols.py
from typing import Tuple, Optional

# Common quote currencies we handle heuristically for no-delimiter symbols
COMMON_QUOTES = [
    "USDT","USD","USDC","EUR","GBP","JPY","BTC","ETH","AUD","CAD","CHF","KRW","TRY","BRL","MXN","ARS","NGN"
]

BASE_ALIASES = {
    "XBT": "BTC",   # Kraken's Bitcoin code
    "XETH": "ETH",  # (rare, but seen in some venues)
}

def split_pair(exchange: str, sym: str) -> Tuple[str, Optional[str]]:
    """
    Return (base, quote) from an exchange-specific symbol string.
    Works for: Kraken (XBT/USDT), Coinbase (BTC-USD), Binance-like (BTCUSDT).
    If quote can't be found, returns (sym, None).
    """
    s = sym.strip().upper()
    ex = (exchange or "").strip().lower()

    if ex == "kraken" and "/" in s:
        base, quote = s.split("/", 1)
    elif ex in {"coinbase","coinbasepro","coinbase-advanced"} and "-" in s:
        base, quote = s.split("-", 1)
    else:
        # Binance-style: BTCUSDT -> split by known quotes
        quote = next((q for q in COMMON_QUOTES if s.endswith(q)), None)
        base = s[:-len(quote)] if quote else s

    # Map aliases (XBT->BTC, etc.)
    base = BASE_ALIASES.get(base, base)
    return base, quote

def normalize_symbol(exchange: str, sym: str) -> str:
    """Return a canonical 'BASE_QUOTE' string if possible, else the original."""
    base, quote = split_pair(exchange, sym)
    return f"{base}_{quote}" if quote else sym.upper()
