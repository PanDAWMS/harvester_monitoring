from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from cric_harvester.filters import Filters

def main():
    settings = path.abspath(path.join(path.dirname(__file__), '..', 'settings.ini'))

    client_mysql = Filters(settings)
    client_mysql.write_filters()

if __name__ == "__main__":
    main()