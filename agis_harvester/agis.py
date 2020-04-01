import requests, json
import pandas as pd
from baseclasses.mysqlbaseclass import MySQLBaseClass

class Agis(MySQLBaseClass):
    def __init__(self, path):
        super().__init__(path)

    def __get_site_info(self):
        url = 'http://atlas-agis-api.cern.ch/request/site/query/list/?json&'
        resp = requests.get(url)
        sites = resp.json()
        return sites


    def __get_pq_info(self):
        url = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas'
        resp = requests.get(url)
        queues = resp.json()
        return queues

    def write_filters(self):
        sites = self.__get_site_info()
        sites_dict = {}
        for site in sites:
            if site['latitude'] == 0 and site['longitude'] == 0:
                # TODO redesign this fragment
                sites_dict[site['rcsite']['name']] = {'site_coordinates':'{0},{1}'.format(0,0)}
            else:
                sites_dict[site['rcsite']['name']] = {'site_coordinates':'{0},{1}'.format(site['latitude'],site['longitude'])}
        queues = self.__get_pq_info()

        computingsites = []

        for queuename, queue in queues.items():
            if queue['status'] in ['brokeroff', 'test', 'online']:
                pq_status = 'ACTIVE'
            else:
                pq_status = 'INACTIVE'
            # computingsites.append([queuename, queue['status'], queue['gocname'], queue['cloud'],
            #                        pq_status, sites_dict[queue['gocname']]['site_coordinates'],
            #                        ','.join(queue['ddmendpoints']), json.dumps(queue)])

            ddm_end = ','.join(queue['ddmendpoints'])

            try:
                del queue["jdladd"]

                sql_update = """INSERT INTO harvester_agis(computingsite, atlas_site, cloud, agis_pq_status, pq_status, site_coordinates, ddm_storages, metadata) 
                VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')
                ON DUPLICATE KEY UPDATE 
                computingsite = VALUES(computingsite), 
                atlas_site = VALUES(atlas_site), 
                cloud = VALUES(cloud),
                agis_pq_status = VALUES(agis_pq_status),
                pq_status = VALUES(pq_status),
                site_coordinates = VALUES(site_coordinates),
                ddm_storages = VALUES(ddm_storages),
                metadata = VALUES(metadata)
                """.format(queuename, queue['gocname'], queue['cloud'], queue['status'],
                                        pq_status, sites_dict[queue['gocname']]['site_coordinates'],
                                        ddm_end, json.dumps(queue))

                self.connection.execute(sql_update)
            except:
                sql_update = """INSERT INTO harvester_agis(computingsite, atlas_site, cloud, agis_pq_status, pq_status, site_coordinates, ddm_storages, metadata) 
                 VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')
                 ON DUPLICATE KEY UPDATE 
                 computingsite = VALUES(computingsite), 
                 atlas_site = VALUES(atlas_site), 
                 cloud = VALUES(cloud),
                 agis_pq_status = VALUES(agis_pq_status),
                 pq_status = VALUES(pq_status),
                 site_coordinates = VALUES(site_coordinates),
                 ddm_storages = VALUES(ddm_storages),
                 metadata = VALUES(metadata)
                 """.format(queuename, queue['gocname'], queue['cloud'], queue['status'],
                            pq_status, sites_dict[queue['gocname']]['site_coordinates'],
                            ddm_end, json.dumps({}))
                self.connection.execute(sql_update)