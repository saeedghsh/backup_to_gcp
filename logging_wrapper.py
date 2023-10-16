import logging
from datetime import datetime


def setup_logging(log_filename: str, directory: str) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    c_handler = logging.StreamHandler()
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    f_handler = logging.FileHandler(f"{directory}/{current_time}_{log_filename}")
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger
