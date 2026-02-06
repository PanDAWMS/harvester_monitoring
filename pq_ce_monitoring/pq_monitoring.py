from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pq_ce_monitoring.pq_infdb import InfluxPQ
from pq_ce_monitoring.pq_timescaledb import TimescalePQ


def main():
    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    #client_infdb = InfluxPQ(settings)
    #client_infdb.write_data(tdelta=60)

    client_tsdb = TimescalePQ(settings)
    client_tsdb.write_data(tdelta=60)

if __name__ == "__main__":
    main()
