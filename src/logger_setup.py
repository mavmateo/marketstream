import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config_setup import get_config



def get_logger(name: str) -> logging.Logger:

    cfg = get_config().logging
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger
    
    logger.setLevel(cfg.level)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if cfg.log_to_file:
        log_path = Path(cfg.log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            log_path,
            maxBytes=cfg.max_bytes,
            backupCount=cfg.backup_count,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)


    logger.propagate = False
    return logger    


