from app.sources.binance_api import get_binance_symbols

def get_symbols(limit=None):
    # Fetch Binance USDT spot trading symbols.
    # Returns a list of dicts: [{ "symbol": "BTCUSDT" }, ...]
    print("Filter 1: Fetching Binance symbols")

    return [{"symbol": s} for s in get_binance_symbols(limit=limit)]
