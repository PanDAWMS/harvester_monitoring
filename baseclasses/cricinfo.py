import requests, os, json, urllib

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
            r = requests.get(
                url,
                cert=(self.cert_file, self.key_file),
                verify=self.ca_certs,
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()

            if isinstance(data, dict):
                return data

            _logger.warning("requests returned non-dict, fallback to urllib")
        except Exception as ex:
            _logger.warning("requests failed, fallback to urllib: %s", ex)
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if isinstance(data, dict):
                return data

            _logger.error("urllib returned non-dict response")

        except Exception as ex:
            _logger.error("urllib failed: %s", ex)

        return None