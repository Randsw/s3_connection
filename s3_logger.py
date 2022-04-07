import logging
import sys
from logging import StreamHandler, Formatter


class S3Logger:

    def __init__(self, logger_name):
        self.logger_name = logger_name
        self.logger = logging.getLogger(logger_name)
        self.handler = StreamHandler(stream=sys.stdout)
        self.handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)
