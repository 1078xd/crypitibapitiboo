import pandas as pd

def queryset_to_df(queryset):
    df = pd.DataFrame.from_records(queryset.values())

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)

    return df
