from datetime import datetime, date

# Convert ANY supported value into a Python date

def iso_to_date(value) -> date:
    # Converts:
    #   - '2024-01-12'
    #   - 1700000000        (seconds)
    #   - 1700000000000     (milliseconds)
    #   - '1700000000'      (numeric string)
    # into a `datetime.date`.

    # Already a date
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    # Already a datetime
    if isinstance(value, datetime):
        return value.date()

    # If it's a string
    if isinstance(value, str):

        # CASE 1: ISO format string "YYYY-MM-DD"
        if "-" in value:
            return datetime.strptime(value, "%Y-%m-%d").date()

        # CASE 2: numeric string
        if value.isdigit():
            ts = int(value)
            return _timestamp_to_date(ts)

        raise ValueError(f"Unrecognized date string: {value}")

    # If it's a numeric timestamp
    if isinstance(value, (int, float)):
        return _timestamp_to_date(value)

    raise TypeError(f"Cannot convert '{value}' of type {type(value)} to date")

# Helper: timestamp → date
def _timestamp_to_date(ts: int | float) -> date:
    # Milliseconds → convert to seconds
    if ts > 1e12:
        ts /= 1000

    return datetime.utcfromtimestamp(ts).date()

# Convert Python date/datetime/timestamp to ISO string
def date_to_iso(value) -> str:
    # Converts:
    #     date → "YYYY-MM-DD"
    #     datetime → "YYYY-MM-DD"
    #     timestamp → "YYYY-MM-DD"
    #     string → returned as-is (if already ISO)
    if isinstance(value, str):
        return value

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, (int, float)):
        return _timestamp_to_date(value).isoformat()

    raise TypeError(f"Cannot convert '{value}' of type {type(value)} to ISO string")

def today_utc() -> date:
    return datetime.utcnow().date()
