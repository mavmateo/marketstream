import asyncio
import json
import websockets


WS_URL = "wss://stream.binance.com:9443/ws"

STREAMS = ["btcusdt@bookTicker",
           "ethusdt@bookTicker",
           "solusdt@bookTicker"]




async def main():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": STREAMS,
            "id": 1,
        }))

        print(await ws.recv())


        while True:
            data = json.loads(await ws.recv())
            

            if "s" in data and "b" in data and "a" in data and "c" in data:
                symbol = data["s"]
                price = data["c"]
                bid = float(data["b"])
                ask = float(data["a"])
                spread = ask - bid

                print(
                    f"{symbol:<8}"
                    f"Price:{price:,.2f}"
                    f"Bid: {bid:,.2f}"
                    f"Ask: {ask:,.2f}"
                    f"Spread: {spread:8f}"
                    
                    )
                
asyncio.run(main())                

