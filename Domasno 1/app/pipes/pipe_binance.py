from app.filters.filter1_symbols import get_symbols
from app.filters.filter2_lastdate import check_last_dates
from app.filters.filter3_download import update_missing_data

def run_pipe_binance(coin_limit, days_back):

    symbols = get_symbols(limit=coin_limit)
    dated = check_last_dates(symbols)
    update_missing_data(dated, days_back=days_back)

