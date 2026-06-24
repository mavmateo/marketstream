import logging

from collections import deque

logger = logging.getLogger(__name__)


VOLUME_WINDOW = 20
PRICE_WINDOW = 20
VOLUME_MULTIPLIER = 3.0
PRICE_MULTIPLIER = 2.5
MIN_HISTORY = 10


window = deque(maxlen= 20)
window.append(100)
window.append(200)

average = sum(window) / len(window)


class AnomalyDetector: 

    def __init__(self,
                 symbol : deque):

        





        def detect(tick) -> dict:  
            logger.info("="*85)




        def _get_history(symbol, store) -> deque:   
            logger.info("="*85)  
        
