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
    def __init__(self, field_name, width, print_name=None):
        self.field_name = field_name
        self.width = width
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

    def add_column(self, field_name, width, print_name=None):
        ci = ColumnInfo(field_name, width, print_name)
        self.column_info.append(ci)

    def get_columnar_print_str(self, col, datum):
        if col.width == None:
            format_str = "{:<s}"
        else:
            format_str = "{:<" + "{:d}".format(col.width) + "} "
        return format_str.format(datum)

    def get_csv_print_str(self, datum):
        return "'{}',".format(datum)

    def print_datum(self, datum_map):
        outstr = ""
        for col in self.column_info:
            if self.separator == " ":
                outstr += self.get_columnar_print_str(col, datum_map[col.field_name])
            else:
                outstr += self.get_csv_print_str(datum_map[col.field_name])
        print(outstr)

    def print_header(self):
        outstr = ""
        for col in self.column_info:
            if self.separator == " ":
                if col.width == None or len(col.field_name) > col.width:
                    col.width = len(col.field_name)
                outstr += self.get_columnar_print_str(col, col.print_name)
            else:
                outstr += self.get_csv_print_str(col.print_name)
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
    parser.add_argument('-S', action='store',
                        help="Space-separated list of stations for which to get data.  Stations are specified via their" 
                             "char id.  If no stations are given or this argument is omitted, the script will return"
                             "a list of all dataloggers.",
                        nargs="*",
                        dest='stations')
    parser.add_argument('-c', action='store_true',
                        help="Print out data as a CSV instead of a columnar format.",
                        dest='print_csv')
    parser.add_argument('-H', action='store_true',
                        help="Print a header row.",
                        dest='print_header')
    parser.add_argument('-q', action='store_true',
                        help="Print query in lieu of outputting data from DB.",
                        dest='print_query')

    args = parser.parse_args()

    if (args.print_csv or args.print_header) and args.print_query:
        sys.stderr.write("You provided command line arguments that specify both printing the mySQL query and "
                         "provide instructions on how to format data output.  This is contradictory!\n")
        exit(1)

    return args, dbinfo

def process_query(dbinfo, args, query):
    # Just print the query if that's all we're doing.
    if args.print_query:
        print(query)
        return None

    return dbinfo.do_query(query)


def main():
    args, dbinfo = configure_script()

    rp = ResultPrinter("," if args.print_csv else " ")
    if args.stations == None or len(args.stations) == 0:

        query = "SELECT DL.id, DL.datalogger_name, DL.datalogger_char_id, DL.datalogger_num_id, WDS.title " \
                "FROM weatherstations_datalogger AS DL " \
                "LEFT JOIN weatherdata_station as WDS " \
                "ON DL.station_id = WDS.id"

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('id', 3)
            rp.add_column('datalogger_char_id', 5)
            rp.add_column('datalogger_name', 45)
            rp.add_column('datalogger_num_id', 5)
            rp.add_column('title', None, 'station_title')

            data = list(cursor.fetchall())
            data.sort(key=lambda ele: ele['id'])
            if args.print_header:
                rp.print_header()
            for ele in data:
                rp.print_datum(ele)

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
