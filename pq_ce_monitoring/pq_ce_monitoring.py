from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from infdb import Influx
from mysql import Mysql

def main():

    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    client_infdb = Influx(settings)
    #client_infdb.write_data(tdelta=60)
    client_infdb.write_data_tmp(tdelta=60)

    #client_mysql = Mysql(settings)
    #client_mysql.write_data(tdelta=60)

if __name__ == "__main__":
    main()