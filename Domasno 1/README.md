Crypitibapitiboo is a Python-based automated data pipeline designed to collect, process, and store historical cryptocurrency market data. 
The system retrieves daily OHLCV (Open, High, Low, Close, Volume) data from the Binance public API, 
processes the data using a modular Pipe-and-Filter architecture, and stores the results in a unified CSV dataset.

The application focuses on scalability, clarity, and automation. 
It uses multithreading to download data for hundreds of symbols efficiently and is structured so that future extensions can be added easily.
