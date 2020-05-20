import logging


def simple_logger(logger_name='root'):
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.setLevel(logging.INFO)
    log.addHandler(stream_handler)

    return log
