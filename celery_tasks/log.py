import logging
import os
import recommendations.config

LOG_PATH = recommendations.config.CELERY_LOG_PATH

def get_file_logger(filename, level=logging.INFO):
    filepath = LOG_PATH + filename

    logger = logging.getLogger(filename)
    logger.setLevel(level)

    filehandler = logging.FileHandler(filepath)
    filehandler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)

    return logger


def get_console_logger(level=logging.DEBUG):
    logger = logging.getLogger()
    logger.setLevel(level)

    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(level)

    formatter = logging.Formatter('[%(levelname)s] %(message)s')
    streamhandler.setFormatter(formatter)

    logger.addHandler(streamhandler)

    return logger
