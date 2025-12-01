def main():
    import time
    from app.pipes.pipe_binance import run_pipe_binance

    COIN_LIMIT = 1000
    DAYS_BACK = 3650

    print("Starting Crypitibapitiboo")

    start = time.perf_counter()
    run_pipe_binance(COIN_LIMIT, DAYS_BACK)
    end = time.perf_counter()

    print(f"TOTAL TIME: {end - start:.2f} seconds")
    print("All pipes finished!")
