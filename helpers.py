import pathlib
import logging
from logging.handlers import RotatingFileHandler

FDIR = pathlib.Path(__file__).parent.resolve()
LOG_FILE = FDIR / 'logs' / 'twitter_log.log'


def simple_logger(logger_name='root'):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(LOG_FILE.as_posix(), maxBytes=1048576)

    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    stream_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)

    log.addHandler(stream_handler)
    log.addHandler(file_handler)

    return log
