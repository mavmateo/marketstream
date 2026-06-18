import json
import logging

logger = logging.getLogger(__name__)


STRONG_BODY_THRESHOLD = 0.70
WEAK_BODY_THRESHOLD = 0.40

def _classify(tick: dict) -> tuple[str, float]:
    logger.info("="*75)
    logger.info("Classifying ticks....")

    direction = "NEUTRAL" 

    confidence = tick['body_size'] / tick['price_range']


    if tick.get("price_range", 0) == 0:
     return "NEUTRAL", 0.0

    if tick['confidence'] > STRONG_BODY_THRESHOLD and tick['is_bullish']:
        direction = "STRONG_BULLISH"
    elif tick['confidence'] > STRONG_BODY_THRESHOLD and not tick['is_bullish']:
        direction = "STRONG_BEARISH"
    elif tick['confidence'] > WEAK_BODY_THRESHOLD and tick['is_bullish']:
        direction = "WEAK_BULLISH"
    elif tick['confidence'] > WEAK_BODY_THRESHOLD and not tick['is_bullish']:
        direction = "WEAK_BEARISH"
    else:
        direction = "NEUTRAL"  

    return confidence, direction



def detect(tick: dict) -> dict:
    logger.info("="*75)
    logger.info("Detecting trend signals.....")

    direction, confidence =  _classify(tick)

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





        

    
