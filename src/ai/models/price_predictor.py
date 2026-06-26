import logging
import numpy as np

from collections import deque
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score as calculate_r2


logger = logging.getLogger(__name__)


PREDICTION_WINDOW = 20
MIN_HISTORY = 10




class PricePredictor:

    def __init__(self) -> None:
        self.close_prices_symbol = {}
        self._model = LinearRegression()
    

    def _get_history(self, symbol: str, store: dict, maxlen: int) -> deque:   
        logger.info("="*85)  
        logger.info("Fetching recent symbol close price history....")

        if symbol not in store:
            store[symbol] =  deque(maxlen=maxlen)
        return store[symbol] 




    def predict(self, tick) -> dict:
        logger.info("="*85) 
        logger.info("*Predicting price based on recent symbol close price history....*")

        close_price_history = self._get_history(tick["symbol"], self.close_prices_symbol, PREDICTION_WINDOW)

        if len(close_price_history) < MIN_HISTORY:
            close_price_history.append(tick["close"])
           
            return {
                    "time":        tick["timestamp"],
                    "symbol":      tick["symbol"],
                    "market":      tick["market"],
                    "signal_type": "anomaly",
                    "direction":   "INSUFFICIENT_DATA",
                    "confidence":  0.0,
                    "predicted_price": None,
                    "details":     {"reason": "not enough history yet",
                                     "candles_seen": len(close_price_history)}
             }
        

        
        prices = np.array(close_price_history)
        X = np.arange(len(prices)).reshape(-1, 1)
        y = prices

        self._model.fit(X, y)

        next_step = np.array([[len(prices)]])
        predicted_price = self._model.predict(next_step)[0]


        price_delta = predicted_price - tick["close"]

        close_price_history.append(tick["close"])


        confidence = calculate_r2(y , self._model.predict(X))
        confidence = max(0.0, confidence)

        signal= {
                        "time" : tick["timestamp"],
                        "symbol" : tick["symbol"],
                        "market" : tick["market"],
                        "signal_type" : "prediction",
                        "direction" : "UP" if predicted_price > tick["close"] else "DOWN",
                        "confidence" : confidence,
                        "predicted_price" : float(predicted_price),
                        "details": {
                        "current_close":   tick["close"],
                        "predicted_close": float(predicted_price),
                        "price_delta" : float(price_delta),
                        "window_size" : len(prices),
                        "model": "linear_regression"
                            }
                    }  

        return signal 






         
    