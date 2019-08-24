from os import sys, path
from infdb import Influx

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def main():
    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    client_infdb = Influx(settings)
    client_infdb.write_data_tmp(tdelta=60*8)

if __name__ == "__main__":
    main()
