import os
import logging
from datetime import datetime
from utilities.logging_wrapper import setup_logging

def test_logging_setup(tmpdir):
    log_filename = 'test.log'
    directory = str(tmpdir)
    logger = setup_logging(log_filename, directory)

    assert logger.level == logging.INFO
    assert len(logger.handlers) == 2
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            assert handler.level == logging.INFO
            assert handler.formatter._fmt == '%(asctime)s - %(levelname)s - %(message)s'
        elif isinstance(handler, logging.FileHandler):
            assert handler.level == logging.INFO
            assert handler.formatter._fmt == '%(asctime)s - %(levelname)s - %(message)s'
            current_time = datetime.now().strftime('%Y%m%d%H%M%S')
            expected_log_file = f"{directory}/{current_time}_{log_filename}"
            assert os.path.exists(expected_log_file)