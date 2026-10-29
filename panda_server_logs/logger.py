import os
import logging


class ServiceLogger:
    def __init__(self, name, file, loglevel='INFO'):
        dir_logs = os.path.dirname(os.path.realpath(file)) + '/logs/'
        os.makedirs(dir_logs, exist_ok=True)
        self.dirpath = dir_logs
        self.logger = self.__get_logger(loglevel, name)

    # private method
    def __get_logger(self, loglevel, name=__name__, encoding='utf-8'):
        log = logging.getLogger(name)
        log.setLevel(loglevel)

        formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-1s %(message)s')

        file_name = self.dirpath + name + '.log'

        fh = logging.FileHandler(file_name, mode='a', encoding=encoding)
        fh.setFormatter(formatter)
        log.addHandler(fh)

        return log
