from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from gahp_monitoring.gahp_infdb import InfluxDbGahp

def main():
    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    client_infdb = InfluxDbGahp(settings)
    client_infdb.write_data_tmp(tdelta=60 * 8)


if __name__ == "__main__":
    main()
