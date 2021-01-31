import logging

from .singleton import Singleton


class CustomLogger(object, metaclass=Singleton):
    __rootLogger = logging.getLogger()
    __rootLogger.setLevel(logging.INFO)

    @classmethod
    def get_logger(cls, name):
        cls.__rootLogger.info("{0}.get_logger(): Inside".format(cls.__name__))
        try:
            logger = logging.getLogger(name)
            logger.setLevel(logging.DEBUG)
            c_handler = logging.StreamHandler()
            c_handler.setLevel(logging.INFO)
            c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            c_handler.setFormatter(c_format)
            logger.addHandler(c_handler)
            return logger
        except Exception as e:
            cls.__rootLogger.error(e)

        cls.__rootLogger.info("{0}.get_logger(): Out".format(cls.__name__))
