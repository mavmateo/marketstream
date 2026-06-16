import json
import logging

logger = logging.getLogger(__name__)


STRONG_BODY_THRESHOLD = 0.70
WEAK_BODY_THRESHOLD = 0.40

def detect(tick: dict) -> dict:
    logger.info("="*75)

    signal = []
    signal.append(tick)
    tick['confidence'] = tick['body_size'] / tick['price_range']
    tick['predicted_price'] = None
    tick['direction'] = None
    tick['signal_type'] = None
    tick['details'] = {
        tick['confidence'], 
        tick['upper_shadow'], 
        tick['lower_shadow']
    }

    logger.info(
        "[%s] %s C=%.2f P=%.2f D=%.2f C=%.2f vol=%.3f",
        tick['timestamp'], tick['symbol'],
        tick['confidence'], tick['direction'],
        tick['predicted_price'],  tick['signal_type'], tick['details']
    )




def _classify(tick: dict) -> tuple[str, float]:
    logger.info("="*75)
    
