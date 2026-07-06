import datetime
import logging





logger = logging.getLogger(__name__)



class OhlcvSchema:
    market: str
    timestamp: datetime
    open: float
    high: float
    low : float
    close: float
    price : float
    volume: float
    interval: int

    required_fields = ["symbol","market","timestamp",
            "open","high","low","close","price","volume", "interval"]   

    numeric_fields = ["open", "close", "high", "low", "price", "volume", "interval"] 

    @staticmethod
    def validate(tick: dict) -> bool:
      if not OhlcvSchema._check_shape(tick):
          return False
      if not OhlcvSchema._check_type(tick):
          return False
      if not OhlcvSchema._check_logic(tick):
          return False
      return True


    @staticmethod
    def _check_shape(tick: dict) -> bool:
        required_fields = ["symbol","market","timestamp",
            "open","high","low","close","price","volume", "interval"]
        missing = [f for f in required_fields if not f in tick]
        if missing:
            logger.warning("Tick missing fields: %s", missing)
            return False
        return True
    
    @staticmethod 
    def _check_type(tick: dict) -> bool:
        for field in OhlcvSchema.numeric_fields:
            if not isinstance(tick[field], (int, float)):
                logger.warning("Tick rejected- field '%s' is '%s', expected numeric",
                               field, type(tick[field]).__name__)
                return False
            if not isinstance(tick["symbol"], str) or not tick["symbol"]:
                logger.warning("Tick rejected - symbol missing or not a string")
                return False
            if not isinstance(tick["timestamp"], str) or not tick["timestamp"]:
                logger.warning("Tick rejected - timestamp missing or not a string")
                return False
            return True
                

    @staticmethod
    def _check_logic(tick: dict) -> bool:
        if tick["high"] < tick["low"]:
            logger.warning("Tick rejected - high < low for %s", tick.get("symbol"))
            return False
        if tick["high"] < tick["open"] or tick["high"] < tick["close"]:
            logger.warning("Tick rejected - high not highest for %s", tick.get("symbol"))
            return False    
        if tick["low"] > tick["open"] or tick["low"] > tick["close"]:
            logger.warning("Tick rejected - low not lowest for %s", tick.get("symbol"))
            return False
       
        if tick["volume"] < 0:
            logger.warning("Tick rejected - negative volume negative for %s", tick.get("symbol"))
            return False
        if tick["price"]  <= 0 :
            logger.warning("Tick reject non positive price for %s", tick.get("symbol")) 
            return False
        return True 
              

    



        




