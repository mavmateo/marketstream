import json
import logging

logger = logging.getLogger(__name__)


STRONG_BODY_THRESHOLD = 0.70
WEAK_BODY_THRESHOLD = 0.40

def _classify(tick: dict) -> tuple[str, float]:
    logger.info("="*75)
    logger.info("Classifying ticks....")

    direction = "NEUTRAL" 

    


    if tick.get("price_range", 0) == 0:
     return "NEUTRAL", 0.0
    
    confidence = tick['body_size'] / tick['price_range']

    if confidence > STRONG_BODY_THRESHOLD and tick['is_bullish']:
        direction = "STRONG_BULLISH"
    elif confidence > STRONG_BODY_THRESHOLD and not tick['is_bullish']:
        direction = "STRONG_BEARISH"
    elif confidence > WEAK_BODY_THRESHOLD and tick['is_bullish']:
        direction = "WEAK_BULLISH"
    elif confidence > WEAK_BODY_THRESHOLD and not tick['is_bullish']:
        direction = "WEAK_BEARISH"
    else:
        direction = "NEUTRAL"  

    return confidence, direction



def detect(tick: dict) -> dict:
    logger.info("="*75)
    logger.info("Detecting trend signals.....")

    confidence, direction =  _classify(tick)

    signal= {
        "time" : tick["timestamp"],
        "symbol" : tick["symbol"],
        "market" : tick["market"],
        "signal_type" : "trend",
        "direction" : direction,
        "confidence" : confidence,
        "predicted_price" : None,
            "details": {
        "body_ratio":   confidence,
        "upper_shadow": tick["upper_shadow"],
        "lower_shadow": tick["lower_shadow"],
          }
   
     }

    logger.info(
    "[%s] %s  direction=%s  confidence=%.4f",
    signal["time"], signal["symbol"],
    signal["direction"], signal["confidence"],
)

    return signal





        

    
