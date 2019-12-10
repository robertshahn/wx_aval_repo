#!/usr/local/bin/python3

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------

import argparse
import configparser
import pymysql
import sys

# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURATION and INITIALIZATION
# ---------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------------------------------------------------


class DBInfo:
    def __init__(self, hostname, username, password, db_name):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.db_name = db_name

        # Open database connection
        self.db = pymysql.connect(self.hostname, self.username, self.password, self.db_name)

    def __del__(self):
        # disconnect from server
        self.db.close()

    def do_query(self, query):
        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()

        # execute SQL query using execute() method.
        cursor.execute(query)

        return cursor


# ---------------------------------------------------------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------------------------------------------------------


def configure_script():
    # Read in the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    dbinfo = DBInfo(config['DEFAULT']['DB_HOSTNAME'],
                    config['DEFAULT']['DB_USERNAME'],
                    config['DEFAULT']['DB_PASSWORD'],
                    config['DEFAULT']['DB_NAME'])

    # Handle the command line arguments
    parser = argparse.ArgumentParser(description='Provides access to the NWAC database.')

    # TODO change to a mode-based command line argument
    parser.add_argument('-S', action='store_true',
                        help="List all weather stations.",
                        dest='list_stations')

    parser.add_argument('-q', action='store_true',
                        help="Print query in lieu of outputting data from DB.",
                        dest='print_query')

    args = parser.parse_args()

    return args, dbinfo

def process_query(dbinfo, args, query):
    # Just print the query if that's all we're doing.
    if args.print_query:
        print(query)
        return None

    return dbinfo.do_query(query)

def main():
    args, dbinfo = configure_script()

    query = None
    if args.list_stations == True:
        query = "SELECT id,title,code FROM weatherdata_station"

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            data = list(cursor.fetchall())
            data.sort(key=lambda ele: ele[0])
            for (id, title, code) in data:
                # make sure the codes are as we expect them to be, i.e., osoXXX, where XXX is the
                # identifying string
                if len(code) != 6 or code[0:3] != 'oso':
                    sys.stderr.write("Unexpected format for station code: {}\n".format(code))
                print("{:3d} {:3s} {:s}".format(id, code[3:], title))

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
