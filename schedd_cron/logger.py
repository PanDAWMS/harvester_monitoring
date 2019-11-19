import logging

class ServiceLogger:
    def __init__(self, name, logroot):
        self.logger = self.__get_logger(name)
        self.logroot = logroot
    # private method
    def __get_logger(self, name=__name__, encoding='utf-8'):
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-1s %(message)s')

        file_name = self.logroot + name + '.log'

        fh = logging.FileHandler(file_name, mode='a', encoding=encoding)
        fh.setFormatter(formatter)
        log.addHandler(fh)

        return log