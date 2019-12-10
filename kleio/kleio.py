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
        cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)

        # execute SQL query using execute() method.
        cursor.execute(query)

        return cursor


class ColumnInfo:
    def __init__(self, field_name, format_str, print_name=None):
        self.field_name = field_name
        self.format_str = format_str
        if print_name is None:
            self.print_name = field_name
        else:
            self.print_name = print_name


class ResultPrinter:
    def __init__(self, separator = " "):
        if separator != " " and separator != ",":
            sys.stderr.write("Unexpected separator passed to ResultPrinter. <{}>".format(separator))
            exit(1)
        self.separator = separator

        self.column_info = list()

    def add_column(self, field_name, format_str, print_name=None):
        ci = ColumnInfo(field_name, format_str, print_name)
        self.column_info.append(ci)

    def print_datum(self, datum_map):
        outstr = ""

        for col in self.column_info:
            if self.separator == " ":
                outstr += col.format_str.format(datum_map[col.field_name]) + " "
            else:
                outstr += "'{}',".format(datum_map[col.field_name])

        print(outstr)

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

        query = "SELECT DL.id, DL.datalogger_name, DL.datalogger_char_id, DL.datalogger_num_id, WDS.title " \
                "FROM weatherstations_datalogger AS DL " \
                "LEFT JOIN weatherdata_station as WDS " \
                "ON DL.station_id = WDS.id"

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp = ResultPrinter()
            rp.add_column('id', '{:3d}')
            rp.add_column('datalogger_char_id', '{:5s}')
            rp.add_column('datalogger_name', '{:45s}')
            rp.add_column('datalogger_num_id', '{:<5d}')
            rp.add_column('title', '{:s}', 'station_title')

            data = list(cursor.fetchall())
            data.sort(key=lambda ele: ele['id'])
            for ele in data:
                rp.print_datum(ele)

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
