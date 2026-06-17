import json
import logging

logger = logging.getLogger(__name__)


STRONG_BODY_THRESHOLD = 0.70
WEAK_BODY_THRESHOLD = 0.40

def detect(tick: dict) -> dict:
    logger.info("="*75)
    logger.info("Detecting trend signals.....")

    signal = {
        "time": tick["timestamp"],
        "symbol": tick["symbol"],
        "confidence": tick['body_size'] / tick['price_range'],
        "direction" : 
    }

    tick['confidence'] = tick['body_size'] / tick['price_range']
    tick['predicted_price'] = None
    tick['direction'] = None
    tick['signal_type'] = None
    signal["details"] = {
    "body_ratio":    confidence,
    "upper_shadow":  tick["upper_shadow"],
    "lower_shadow":  tick["lower_shadow"],
     }

    logger.info(
        "[%s] %s C=%.2f P=%.2f D=%.2f C=%.2f vol=%.3f",
        signal["timestamp"], signal["symbol"],
        signal["confidence"], signal["direction"], 
        signal["predicted_price"], signal["signal_type"], signal["details"]
    )

    return signal




def _classify(tick: dict) -> tuple[str, float]:
    logger.info("="*75)
    logger.info("Classifying ticks....")
    if tick.get("price_range", 0) == 0:
     return "NEUTRAL", 0.0

    tick['confidence'] = tick['body_size'] / tick['price_range']

    if tick['confidence'] > STRONG_BODY_THRESHOLD and tick['is_bullish'] == "True":
        tick['direction'] = "STONG_BULLISH"

    if tick['confidence'] > STRONG_BODY_THRESHOLD and tick['is_bullish'] == " False":
        tick['direction'] = "WEAK_BULLISH"    

    if  tick['confidence'] < WEAK_BODY_THRESHOLD and tick['is_bullish'] == "False":
        tick['direction'] = "STRONG_BEARISH"

    if tick['confidence'] < WEAK_BODY_THRESHOLD and tick['is_bullish'] == "True":
        tick['direction']  = "WEAK_BEARISH"  

    if tick['confidence'] <  STRONG_BODY_THRESHOLD and tick['body_ratio'] > WEAK_BODY_THRESHOLD:
        tick['direction'] = "NEUTRAL"   

        return tick['confidence'], tick['direction']
        

    
