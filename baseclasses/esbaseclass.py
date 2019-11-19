from configparser import ConfigParser
from elasticsearch import Elasticsearch
from logger import ServiceLogger

_logger = ServiceLogger("elasticsearch", __file__, "ERROR").logger


class EsBaseClass:

    def __init__(self, path):
        self.connection = self.__make_connection(path=path)

    # private method
    def __make_connection(self, path, use_ssl=True, verify_certs=False, timeout=30, max_retries=10,
                          retry_on_timeout=True):
        """
        Create a connection to ElasticSearch cluster
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            eslogin = cfg.get('server', 'login')
            espasswd = cfg.get('server', 'password')
            host = cfg.get('server', 'host')
            port = cfg.get('server', 'port')
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        try:
            connection = Elasticsearch(
                [{'host': host, 'port': int(port)}],
                http_auth=(eslogin, espasswd),
                use_ssl=use_ssl,
                verify_certs=verify_certs,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout,
            )
            return connection
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        return None