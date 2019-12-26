#!/usr/local/bin/python3

# The use of this script is described in
# https://docs.google.com/document/d/1OMhArGSdC8cSmdPu8fiHjUrNR50y1o4MRpr6Po2m5FQ/edit?usp=sharing

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------

import argparse
import configparser
import pytz
import pymysql
import sys
from collections import defaultdict
from datetime import datetime

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

    def print_datum(self, datum_map, sigfigs = 2):
        outstr = ""
        for col in self.column_info:
            print_val = datum_map[col.field_name]
            if isinstance(print_val, float):
                print_val = round(print_val, sigfigs)

            if self.separator == " ":
                outstr += self.get_columnar_print_str(col, print_val)
            else:
                outstr += self.get_csv_print_str(print_val)
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


def parse_dt_str(dt_str):
    date_formats = ["%Y%m%d %H:%M:%S", "%Y%m%d %H:%M", "%Y%m%d"]
    parsed_dt = None
    for cur_format in date_formats:
        try:
            parsed_dt = datetime.strptime(dt_str, cur_format)
            break
        except ValueError:
            pass

    if parsed_dt is None:
        sys.stderr.write("Time must be specified as 'YYYYMMDD [HH:MM[:SS]], where 'HH' is 24 hour time.\n")
        exit(1)

    return parsed_dt


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
    parser.add_argument('-s', action='store',
                        help="Start time specified as 'YYYYMMDD [24 hour time, Pacific].'  If '-s' is specified, '-e' "
                            "must also be specified.  If a start and end time are specified, then we actually get data "
                            "from the database.  Otherwise, this script will return a list what fields can be accessed "
                            "in the database.",
                        dest='start_time')
    parser.add_argument('-e', action='store',
                        help="End time specified as 'YYYYMMDD [24 hour time, Pacific].  Data is queried up to but not "
                             "including the specified time.  See information on '-s' argument for more information.",
                        dest='end_time')
    #todo Sum or average based on what the datafield is, i.e., battery voltage should be averged, precip summed.
    parser.add_argument('--bin', action='store',
                        help="Bin by days or by half-days, summing all data fields.",
                        choices=['daily', 'ampm'],
                        dest='do_binning')

    parser.add_argument('-L', action='store',
                        help="Space-separated list of stations for which to get data.  Stations are specified via "
                            "their <char id>.  If no stations are given or this argument is omitted, the script will "
                            "return a list of all dataloggers.",
                        nargs="*",
                        dest='stations')
    parser.add_argument('-S', action='store',
                        help="Space-separated list of sensor types for which to get data.  If one or more stations "
                            "are given with the '-L' argument and either no sensors are given or this argument is "
                            "ommitted, the script will return a list of all sensor types for the specified stations.",
                        nargs="*",
                        dest="sensors")

    parser.add_argument('--csv', action='store_true',
                        help="Print out data as a CSV instead of a columnar format.",
                        dest='print_csv')
    parser.add_argument('--header', action='store_true',
                        help="Print a header row.",
                        dest='print_header')
    parser.add_argument('--sql', action='store_true',
                        help="Print query in lieu of outputting data from DB.",
                        dest='print_query')

    args = parser.parse_args()

    if args.start_time is not None or args.end_time is not None:
        # Make sure we have both a start and end time if either is specified
        if args.start_time is None or args.end_time is None:
            sys.stderr.write("If a start time ('-s') is specified, an end time must also be specified ('-e'), and "
                             "vice versa.\n")
            exit(1)

        # Turn the strings into datetimes
        args.start_time = parse_dt_str(args.start_time)
        args.end_time = parse_dt_str(args.end_time)

        # Convert the command line args to UTC since that's what's stored in the DB.
        args.start_time = pytz.timezone("US/Pacific").localize(args.start_time).astimezone(pytz.utc)
        args.end_time = pytz.timezone("US/Pacific").localize(args.end_time).astimezone(pytz.utc)

        # Make sure we have also specified stations and sensors
        if args.stations is None or len(args.stations) == 0 or args.sensors is None or len(args.sensors) == 0:
            sys.stderr.write("When we have specified a date range ('-s' and '-e'), we also need a list of stations "
                             "('-L') for which to get data and a list of the desired fields ('-S') to query.\n")
            # Don't exit here.  Instead, let's allow the script to list stations or sensors, as appropriate based on the
            # other arguments
            args.start_time = None
            args.end_time = None

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

def bin_data(bin_type, data):
    NON_DATA_FIELDS = {'station', 'time'}

    # station -> time_key -> data
    out_data_map = defaultdict(dict)

    # Put the data into bins
    for ele in data:
        dl_id = ele["station"]

        if bin_type == "daily":
            time_key_format = "%Y%m%d"
        else:
            time_key_format = "%Y%m%d-%p"
        time_key = ele['time'].strftime(time_key_format)

        # If we've never seen this dl_id-time_key combo, initialize our data to '0.'
        if dl_id not in out_data_map or time_key not in out_data_map[dl_id]:
            out_data_map[dl_id][time_key] = dict()

            for k, v in ele.items():
                if k == 'station':
                    new_v = v
                elif k == 'time':
                    new_v = time_key
                else:
                    new_v = 0.0

                out_data_map[dl_id][time_key][k] = new_v

        # Now add in the elements
        for k, v in ele.items():
            if k not in NON_DATA_FIELDS:
                out_data_map[dl_id][time_key][k] += v

    # Turn the output dict into a list
    #todo Is there a more Pythonic way to do this?
    output_list = list()
    for dl_id, dl_data in out_data_map.items():
        for time_key, sensor_data in dl_data.items():
            output_list.append(sensor_data)

    return output_list

def main():
    args, dbinfo = configure_script()

    rp = ResultPrinter("," if args.print_csv else " ")

    # If we have a start time, we must want to get data, so let's do just that.
    if args.start_time is not None:
        sensor_str = ", ".join(map(lambda x: "M." + x, args.sensors))
        station_str = " OR ".join(map(lambda x: "DL.datalogger_char_id='{}'".format(x), args.stations))

        query = "SELECT DL.datalogger_char_id AS 'station', CONVERT_TZ(M.timecode, 'UTC', 'US/Pacific') AS 'time', "\
                "{sensor} " \
                "FROM weatherstations_datalogger DL " \
                "INNER JOIN weatherstations_measurement M " \
                "ON M.data_logger_id = DL.id " \
                "WHERE M.timecode>='{start_dt}' AND M.timecode<'{end_dt}' AND ({stations})".format(
            sensor=sensor_str,
            start_dt=args.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_dt=args.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            stations=station_str)

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('station', 5)
            rp.add_column('time', 18)
            for sensor in args.sensors:
                rp.add_column(sensor, 25)

            data = list(cursor.fetchall())
            if args.do_binning is not None:
                data = bin_data(args.do_binning, data)
            data.sort(key=lambda ele: ele['station'])

            if args.print_header:
                rp.print_header()
            for ele in data:
                if args.do_binning is None:
                    ele['time'] = ele['time'].strftime('%Y%m%d %H:%M:%S')
                rp.print_datum(ele)

    # If no stations are specified, get a list of all the stations
    elif args.stations is None or len(args.stations) == 0:

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
    # We have at least one station specified but no start and end date, so we must want a list of all the sensors at
    # those stations.
    else:
        query = "SELECT DL.datalogger_char_id, T.sensortype_name, T.field_name FROM weatherstations_datalogger DL " \
                "INNER JOIN weatherstations_sensor S ON S.data_logger_id = DL.id " \
                "INNER JOIN weatherstations_sensortype T ON S.sensor_type_id = T.id " \
                "WHERE "

        for i, station in enumerate(args.stations):
            if i >= 1:
                query += " OR "
            query += "DL.datalogger_char_id='{}'".format(station)

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('datalogger_char_id', 5)
            rp.add_column('sensortype_name', 25)
            rp.add_column('field_name', 25)

            data = list(cursor.fetchall())
            data.sort(key=lambda ele: ele['datalogger_char_id'])
            if args.print_header:
                rp.print_header()
            for ele in data:
                rp.print_datum(ele)

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()

