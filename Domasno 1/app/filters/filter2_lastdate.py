def check_last_dates(symbols):
    # When using a single master CSV, we do not check last saved dates.
    # Always return last_date=None.
    print("Filter 2: Skipping last-date check")

    result = []
    for sym in symbols:
        result.append({
            "symbol": sym["symbol"],
            "last_date": None
        })

    return result
