import logging

from collections import deque

logger = logging.getLogger(__name__)


VOLUME_WINDOW = 20
PRICE_WINDOW = 20
VOLUME_MULTIPLIER = 3.0
PRICE_MULTIPLIER = 2.5
MIN_HISTORY = 10





class AnomalyDetector: 

    def __init__(self) -> None:
        
        self.volume_symbol = {}
        self.price_ranges_symbol = {}
        

        

    def _get_history(self, symbol: str, store: dict, maxlen: int) -> deque:   
        logger.info("="*85)  
        logger.info("Fetching recent symbol history....")

        if symbol not in store:
            store[symbol] =  deque(maxlen=maxlen)
        return store[symbol]    

   


    def detect(self, tick) -> dict:  
        logger.info("="*85)
        logger.info("Detecting price and volume anomalies from recent history....")
        
        volume_history = self._get_history(tick["symbol"], self.volume_symbol , VOLUME_WINDOW)
        price_range_history = self._get_history(tick["symbol"], self.price_ranges_symbol, PRICE_WINDOW)

        if len(volume_history) < MIN_HISTORY:
             volume_history.append(tick["volume"])
             price_range_history.append(tick["price_range"])
             return {
                    "time":        tick["timestamp"],
                    "symbol":      tick["symbol"],
                    "market":      tick["market"],
                    "signal_type": "anomaly",
                    "direction":   "INSUFFICIENT_DATA",
                    "confidence":  0.0,
                    "predicted_price": None,
                    "details":     {"reason": "not enough history yet",
                                     "candles_seen": len(volume_history)}
             }
             

        avg_volume = sum(volume_history) / len(volume_history)
        avg_price = sum(price_range_history) / len(price_range_history)

        volume_ratio = tick["volume"] / avg_volume if avg_volume > 0 else 0
        price_ratio = tick["price_range"] / avg_price if avg_price > 0 else 0

        is_volume_anomaly = volume_ratio > VOLUME_MULTIPLIER
        is_price_anomaly  = price_ratio > PRICE_MULTIPLIER
        is_anomaly        = is_price_anomaly or is_volume_anomaly

        anomaly_type = None
        if is_volume_anomaly and is_price_anomaly: anomaly_type = "volume_and_price"
        elif is_volume_anomaly: anomaly_type = "volume"
        elif is_price_anomaly: anomaly_type = "price"
        
       
        volume_history.append(tick["volume"])
        price_range_history.append(tick["price_range"])

        signal= {
                            "time" : tick["timestamp"],
                            "symbol" : tick["symbol"],
                            "market" : tick["market"],
                            "signal_type" : "anomaly",
                            "direction" : "ANOMALY" if is_anomaly else "NORMAL",
                            "confidence" : max(volume_ratio, price_ratio),
                            "predicted_price" : None,
                            "details": {
                            "current_volume":   tick["volume"],
                            "average_volume": avg_volume,
                            "volume_ratio" : volume_ratio,
                            "current_price_range": tick["price_range"],
                            "average_price_range": avg_price,
                            "price_ratio" : price_ratio,
                            "anomaly_type": anomaly_type
                              }
                        }  

        return signal   
                    
                







 