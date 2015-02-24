__author__ = 'lsamaha'

import logging

class Log(object):

    def get_logger(self, log_file, debug):
        logger = logging.getLogger('Service.Python.Logger')
        level = logging.INFO
        if debug:
            level = logging.DEBUG
        logger.setLevel(level)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        fh = logging.FileHandler(log_file)
        fh.name = 'File Logger'
        fh.level = level
        fh.formatter = formatter
        logger.addHandler(fh)
        return logger

