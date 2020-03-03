import os
import logging

from pathlib import Path


class ServiceLogger:
    def __init__(self, name, file, loglevel='DEBUG'):
        p = str(Path(file).parent) + '/logs/'
        i = 0
        while True:
            if not os.path.exists(p):
                i = i + 1
                p = str(Path(file).parents[i]) + '/logs/'
            else:
                self.dirpath = p
                break
        self.logger = self.__get_logger(loglevel, name)

    # private method
    def __get_logger(self, loglevel, name=__name__, encoding='utf-8'):
        log = logging.getLogger(name)
        level = logging.getLevelName(loglevel)
        log.setLevel(level)

        formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-1s %(message)s')

        file_name = self.dirpath + name + '.log'

        fh = logging.FileHandler(file_name, mode='a', encoding=encoding)
        fh.setFormatter(formatter)
        log.addHandler(fh)

        return log