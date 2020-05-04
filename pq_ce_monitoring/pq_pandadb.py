import json
from logger import ServiceLogger
from baseclasses.oracledbbaseclass import OracleDbBaseClass

_logger = ServiceLogger("pq_pandadb", __file__).logger


class PandaDBPQ(OracleDbBaseClass):

    def __init__(self, path):
        super().__init__(path)

    def get_running_workers_jobs(self):
        try:
            connection = self.connection

            computingsite_stat = {}

            query = """
            SELECT computingsite, count(status) as nworkers, count(jobstatus) as njobs  
            FROM 
            (SELECT
            ww.computingsite as computingsite, 
            ww.status as status, 
            ww.lastupdate as wlastupdate, 
            ww.starttime as wstarttime, 
            jj.pandaid,
            jj.LASTUPDATE as pidlastupdate,
            ja.jobstatus, 
            ja.computingsite as jobcomputingsite
            FROM ATLAS_PANDA.harvester_workers ww
            LEFT OUTER JOIN atlas_panda.harvester_rel_jobs_workers jj ON ww.harvesterid = jj.harvesterid and ww.workerid = jj.workerid
            LEFT OUTER JOIN atlas_panda.jobsactive4 ja ON jj.pandaid = ja.pandaid )
            WHERE status = 'running' and wlastupdate >= CAST (sys_extract_utc(SYSTIMESTAMP) - interval '10' minute as DATE)
            GROUP BY computingsite
            """

            results = self.__read_query(query, connection)

            for result in results:
                computingsite_stat[result['computingsite']] = {'nworkers':result['nworkers'], 'njobs':result['njobs']}
        except:
            pass
        return computingsite_stat

    def get_running_workers_completed_jobs(self):
        try:
            connection = self.connection

            computingsite_stat = {}

            query = """
            SELECT computingsite, count(status) as nworkers, count(jobstatus) as njobs  
            FROM 
            (SELECT
            ww.computingsite as computingsite, 
            ww.status as status, 
            ww.lastupdate as wlastupdate, 
            ww.starttime as wstarttime, 
            jj.pandaid,
            jj.LASTUPDATE as pidlastupdate,
            ja.jobstatus, 
            ja.computingsite as jobcomputingsite
            FROM ATLAS_PANDA.harvester_workers ww
            LEFT OUTER JOIN atlas_panda.harvester_rel_jobs_workers jj ON ww.harvesterid = jj.harvesterid and ww.workerid = jj.workerid
            LEFT OUTER JOIN atlas_panda.jobsarchived4 ja ON jj.pandaid = ja.pandaid)
            WHERE status = 'running' and wlastupdate >= CAST (sys_extract_utc(SYSTIMESTAMP) - interval '10' minute as DATE)
            GROUP BY computingsite
            """

            results = self.__read_query(query, connection)

            for result in results:
                computingsite_stat[result['computingsite']] = {'nworkers':result['nworkers'], 'njobs':result['njobs']}
        except:
            pass
        return computingsite_stat

    # private method
    def __read_query(self, query, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return self.__rows_to_dict_list(cursor)
        finally:
            if cursor is not None:
                cursor.close()

    # private method
    def __rows_to_dict_list(self, cursor):
        columns = [str(i[0]).lower() for i in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]
