import requests, os

from configparser import ConfigParser
from logger import ServiceLogger

_logger = ServiceLogger("cricinfo", __file__, "ERROR").logger


class CricInfo:

    def __init__(self, path):
        try:
            cfg = ConfigParser()
            cfg.read(path)
            self.cert_file = cfg.get('cert', 'cert_file')
            self.key_file = cfg.get('cert', 'key_file')
            self.ca_certs = cfg.get('cert', 'ca_certs')
        except Exception as ex:
            _logger.error(ex)

    def get_cric_info(self, url):
        try:
            r = requests.get(url, cert=(self.cert_file, self.key_file), verify=self.ca_certs)
            return r.json()
        except Exception as ex:
            _logger.error(ex)