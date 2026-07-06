import time
import logging
import functools

logger = logging.getLogger(__name__)

def log_signal(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000
        logger.debug(
            "%s completed in %.2f ms | symbol=%s direction=%s",
            func.__name__,
            elapsed,
            result.get("symbol"),
            result.get("direction"),
        )
        return result
    return wrapper