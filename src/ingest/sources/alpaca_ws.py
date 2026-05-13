from alpaca.data.live import StockDataStream


stream = StockDataStream(env.ALPACA_API_KEY,env.ALPACA_SECRET_KEY)

async def quote_handler(quote):
    print("Quote Received")
    print(f"Symbol: {quote.symbol}")
    print(f"Bid: {quote.bid_price}")
    print(f"Ask: {quote.ask_price}")
    print(f"Timestamp: {quote.timestamp}")
    print("=" * 60)

    stream.subscribe_quotes(quote_handler, "AAPL", "MSFT", "TSLA") 

stream.run()