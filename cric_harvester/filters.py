import json
from baseclasses.mysqlbaseclass import MySQLBaseClass
from baseclasses.cricinfobaseclass import CricInfo
from logger import ServiceLogger

_logger = ServiceLogger("filters", __file__, "ERROR").logger

class Filters(MySQLBaseClass, CricInfo):
    def __init__(self, path):
        super().__init__(path)

    def read_country_coordinates(self):
        countries = {}
        from os import path
        json_path = path.join(path.dirname(__file__)) + '/' + 'countries.json'

        with open(json_path) as json_file:
            data = json.load(json_file)
            for country in data:
                if country == 'United Kingdom':
                    countries['UK'] = country['latlng']
                if country == 'Russia':
                    countries['Russian Federation'] = country['latlng']
                if country == 'United States':
                    countries['USA'] = country['latlng']
                    countries['United States of America'] = country['latlng']
                countries[country['name']] = country['latlng']
        return countries

    def write_filters(self):
        sites = self.get_site_info()
        sites_dict = {}
        countries = self.read_country_coordinates()

        for sitename, siteinfo in sites.items():
            if siteinfo['latitude'] == 0 and siteinfo['longitude'] == 0:
                # TODO redesign this fragment
                try:
                    sites_dict[siteinfo['rcsite']['name']] = {'site_coordinates':'{0},{1}'
                        .format(countries[siteinfo['country']][0],countries[siteinfo['country']][1])}
                except:
                    sites_dict[siteinfo['rcsite']['name']] = {'site_coordinates':'{0},{1}'.format(0,0)}
            else:
                sites_dict[siteinfo['rcsite']['name']] = {'site_coordinates':'{0},{1}'.format(siteinfo['latitude'],
                                                                                              siteinfo['longitude'])}

        queues = self.get_pq_info()

        for queuename, queue in queues.items():
            if queue['status'] in ['brokeroff', 'test', 'online']:
                pq_status = 'ACTIVE'
            else:
                pq_status = 'INACTIVE'
            #ddm_end = ','.join(queue['ddmendpoints'])
            ddm_end = ''
            try:
                del queue["jdladd"]

                sql_reqeuest = """INSERT INTO pq_info(computingsite, atlas_site, cloud, agis_pq_status, pq_status, site_coordinates, ddm_storages, metadata) 
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

                self.connection.execute(sql_reqeuest)
            except:
                sql_reqeuest = """INSERT INTO pq_info(computingsite, atlas_site, cloud, agis_pq_status, pq_status, site_coordinates, ddm_storages, metadata) 
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
                self.connection.execute(sql_reqeuest)