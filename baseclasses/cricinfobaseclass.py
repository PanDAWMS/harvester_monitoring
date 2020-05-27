import requests

from logger import ServiceLogger

_logger = ServiceLogger("cricinfo", __file__, "ERROR").logger

class CricInfo:
    def __init__(self):
        pass

    def get_site_info(self):
        url = 'http://atlas-cric.cern.ch/api/core/site/query/?json'
        resp = requests.get(url)
        sites = resp.json()
        return sites

    def get_pq_info(self):
        url = 'http://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json'
        resp = requests.get(url)
        queues = resp.json()
        return queues