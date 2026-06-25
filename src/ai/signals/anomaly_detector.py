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
            history =  deque(maxlen)
        return history    

   


    def detect(self, tick) -> dict:  
        logger.info("="*85)
        logger.info("Detecting anomaly from recent symbol history....")
        
        volume_history = self._get_history(tick["symbol"], maxlen=VOLUME_WINDOW)
        price_range_history = self._get_history(tick["symbol"], maxlen=PRICE_WINDOW)

        if not volume_history > MIN_HISTORY :
             return "INSUFFICIENT DATA"
             

        avg_volume = sum(volume_history) / len(volume_history)
        avg_price = sum(price_range_history) / len(price_range_history)


        if tick["volume"] > avg_volume * VOLUME_MULTIPLIER:
                volume_history.append(tick["volume"])

                

        if tick["price_range"] > avg_price * PRICE_MULTIPLIER:
             price_range_history.append(tick["price_range"])

        signal= {
                            "time" : tick["timestamp"],
                            "symbol" : tick["symbol"],
                            "market" : tick["market"],
                            "signal_type" : "anomaly",
                            "direction" : "ANOMALY" if is_anomaly else "NORMAL",
                            "confidence" : ratio,
                            "predicted_price" : None,
                                "details": {
                            "current_volume":   tick["volume"],
                            "average_volume": avg_volume,
                            "multiplier": ratio,
                            "anomaly_type": "volume" if is_anomaly else None,
                              }
                        }  

        return signal   
                    
                







 