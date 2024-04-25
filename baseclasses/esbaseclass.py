from configparser import ConfigParser
from opensearchpy import OpenSearch
from logger import ServiceLogger

_logger = ServiceLogger("opensearch", __file__, "ERROR").logger


class EsBaseClass:

    def __init__(self, path):
        self.connection = self.__make_connection(path=path)

    # private method
    def __make_connection(self, path, verify_certs=True, timeout=2000, max_retries=10,
                          retry_on_timeout=True):
        """
        Create a connection to OpenSearch cluster
        """
        try:
            cfg = ConfigParser()
            cfg.read(path)
            eslogin = cfg.get('esserver', 'login')
            espasswd = cfg.get('esserver', 'password')
            host = cfg.get('esserver', 'host')
            сa_path = cfg.get('esserver', 'capath')
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        try:
            connection = OpenSearch(
                ['https://{0}/os'.format(host)],
                http_auth=(eslogin, espasswd),
                verify_certs=verify_certs,
                timeout=timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout,
                ca_certs=сa_path
            )
            return connection
        except Exception as ex:
            _logger.error(ex)
            print(ex)
        return None