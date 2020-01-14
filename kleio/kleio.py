#!/usr/local/bin/python3

# The use of this script is described in
# https://docs.google.com/document/d/1OMhArGSdC8cSmdPu8fiHjUrNR50y1o4MRpr6Po2m5FQ/edit?usp=sharing

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------

import argparse
import configparser
import sys

import pymysql
import pytz

sys.path.append('../lib/')
import nwac  # noqa: E402

from collections import defaultdict  # noqa: E402
from datetime import datetime, timedelta  # noqa: E402
from enum import Enum, auto  # noqa: E402

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
    def __init__(self, separator=" "):
        if separator != " " and separator != ",":
            sys.stderr.write("Unexpected separator passed to ResultPrinter. <{}>".format(separator))
            exit(1)
        self.separator = separator

        self.column_info = list()

    def add_column(self, field_name, width, print_name=None):
        ci = ColumnInfo(field_name, width, print_name)
        self.column_info.append(ci)

    @staticmethod
    def get_columnar_print_str(col, datum):
        if col.width is None:
            format_str = "{:<s}"
        else:
            format_str = "{:<" + "{:d}".format(col.width) + "} "
        return format_str.format(datum)

    @staticmethod
    def get_csv_print_str(datum):
        return "'{}',".format(datum)

    def print_datum(self, datum_map, sigfigs=2):
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
                if col.width is None or len(col.field_name) > col.width:
                    col.width = len(col.field_name)
                outstr += self.get_columnar_print_str(col, col.print_name)
            else:
                outstr += self.get_csv_print_str(col.print_name)
        print(outstr)


# Helper class for deciding how to bin fields in the 'weatherstations_measurement' table
class BinningOp:
    class Action(Enum):
        NULL = auto()  # Shouldn't get this value
        CHECK_EQUAL = auto()  # Check to make sure all values are equal
        AVERAGE = auto()
        SUM = auto()
        MIN = auto()
        MAX = auto()

    # todo Figure out what action to take for fields that are commented out
    FIELD_MAP = {
        #        'id'                  : Action.PASSTHROUGH,
        'data_logger_id': Action.NULL,
        'time': Action.NULL,  # remapped from 'timecode' in SQL query
        'is_24hr': Action.CHECK_EQUAL,
        'battery_voltage': Action.AVERAGE,
        'temperature': Action.AVERAGE,
        'relative_humidity': Action.AVERAGE,
        'precipitation': Action.SUM,
        'snow_depth': Action.AVERAGE,
        'wind_direction': Action.AVERAGE,
        'wind_speed_average': Action.AVERAGE,
        'wind_speed_minimum': Action.MIN,
        'wind_speed_maximum': Action.MAX,
        'barometric_pressure': Action.AVERAGE,
        'equip_temperature': Action.AVERAGE,
        'solar_pyranometer': Action.AVERAGE,
        #        'snowfall_24_hour'    : Action.AVERAGE,
        #        'intermittent_snow'   : Action.??,
        #        'data_source_id'      : Action.PASSTHROUGH,
        'net_solar': Action.AVERAGE,
        #        'int_hash'            : Action.??,
        'soil_moisture_a': Action.AVERAGE,
        'soil_moisture_b': Action.AVERAGE,
        'soil_moisture_c': Action.AVERAGE,
        'soil_temperature_a': Action.AVERAGE,
        'soil_temperature_b': Action.AVERAGE,
        'soil_temperature_c': Action.AVERAGE
    }

    @staticmethod
    def is_binnable_col(col_name):
        if col_name not in BinningOp.FIELD_MAP.keys() or BinningOp.FIELD_MAP[col_name] == BinningOp.Action.NULL:
            return False
        return True

    @staticmethod
    def mix_in_value(old_value, key, cur_value):

        if key not in BinningOp.FIELD_MAP.keys():
            raise ValueError("Trying to bin a key for which an action has not been defined: {key}".format(
                key=key
            ))
        action = BinningOp.FIELD_MAP[key]

        if action == BinningOp.Action.NULL:
            raise RuntimeError("Trying to bin a key that should never be binned: {key}.".format(
                key=key
            ))
        elif action == BinningOp.Action.CHECK_EQUAL:
            if old_value is None:
                return cur_value

            if old_value != cur_value:
                raise ValueError("Values should be constant for key '{key}', but values changed: '{old}' -> "
                                 "'{new}'.".format(key=key,
                                                   old=old_value,
                                                   new=cur_value
                                                   ))
        elif action == BinningOp.Action.AVERAGE:
            if old_value is None:
                old_value = (0, 0.0)
            return old_value[0] + 1, old_value[1] + cur_value
        elif action == BinningOp.Action.SUM:
            if old_value is None:
                old_value = 0.0
            return old_value + cur_value
        elif action == BinningOp.Action.MIN:
            if old_value is None:
                return cur_value
            return min(old_value, cur_value)
        elif action == BinningOp.Action.MAX:
            if old_value is None:
                return cur_value
            return max(old_value, cur_value)
        else:
            raise RuntimeError("Action '{action}' has been defined but not implemented.".format(
                action=action
            ))

    @staticmethod
    def complete_bin(key, cur_bin):
        action = BinningOp.FIELD_MAP[key]

        if action == BinningOp.Action.NULL:
            raise RuntimeError("Trying to bin a key that should never be binned: {key}.".format(
                key=key
            ))
        elif action == BinningOp.Action.CHECK_EQUAL:
            return cur_bin
        elif action == BinningOp.Action.AVERAGE:
            return cur_bin[1] / cur_bin[0]
        elif action == BinningOp.Action.SUM:
            return cur_bin
        elif action == BinningOp.Action.MIN:
            return cur_bin
        elif action == BinningOp.Action.MAX:
            return cur_bin
        else:
            raise RuntimeError("Action '{action}' has been defined but not implemented.".format(
                action=action
            ))


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

    parser.add_argument('-s', action='store',
                        help="Start time specified as 'YYYYMMDD [HH:MM[:SS]]'.  If a start is specified, then we "
                             "actually get data from the database.  Otherwise, this script will return a list what "
                             "fields can be accessed in the database.",
                        dest='start_time')
    parser.add_argument('-e', action='store',
                        help="End time specified as 'YYYYMMDD [HH:MM[:SS]]'.  Data is queried up to and "
                             "including the specified time.  If a start time is specified ('-s') but this argument is"
                             "omitted, we set the end time to the start time, e.g., a single day analysis.",
                        dest='end_time')
    parser.add_argument('--zone', action='store', default="gmt",
                        help="The timezone for which the date range is specified.  Remember, forecasts are, by default,"
                             "given for 12-hour GMT AM/PMs.  As such, the default value is 'gmt.'",
                        choices=['gmt', 'pacific'],
                        dest='timezone')
    parser.add_argument('--bin', action='store',
                        help="Bin by days or by half-days, summing all data fields.",
                        choices=['daily', 'ampm'],
                        dest='do_binning')

    parser.add_argument('-L', action='store',
                        help="Space-separated list of stations for which to get data.  Stations are specified via "
                             "their AWS ID or Mesowest ID.  See '--id' documentation for more information.  If no "
                             "stations are given or this argument is omitted, the script will return a list of all "
                             "dataloggers.",
                        nargs="*",
                        dest='stations')
    parser.add_argument('-S', action='store',
                        help="Space-separated list of sensor types ('field_name' values)  which to get data.  If one "
                             "or more stations are given with the '-L' argument and either no sensors are given or "
                             "this argument is omitted, the script will return a list of all sensor types for the "
                             "specified stations.",
                        nargs="*",
                        dest="sensors")
    parser.add_argument('--id', action='store', default="aws",
                        help="Whether the IDs specified on the command line are Mesowest ids or AWS ids.  Default is "
                             "AWS.",
                        choices=['aws', 'mesowest'],
                        dest='station_id_type')

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

    if args.stations is not None:
        # If we've gotten Mesowest station IDs, convert them to AWS ids.
        if args.station_id_type == 'mesowest':
            new_stations = list()
            for station in args.stations:
                new_station = nwac.StationNameConversion.MESOWEST_TO_AWS_MAP.get(station)
                if new_station is None:
                    raise ValueError("Invalid station ID specified: {station}".format(station=station))
                new_stations.append(new_station)
            args.stations = new_stations

    if args.start_time is not None:
        # If the end time is not specified, we just set it to the start time.
        if args.end_time is None:
            args.end_time = args.start_time

        # Turn the strings into datetimes
        parsed_start_time = parse_dt_str(args.start_time)
        parsed_end_time = parse_dt_str(args.end_time)

        # If we only got dates (YYYYMMDD) on the command line, we need to push the times around to get the right data
        # out of the database.  This is necessary because data is timestamped with the time of the end of the period
        # for which data has been collected...
        if len(str(args.start_time)) == 8:
            # ... so for the start_time, we don't want DB data for YYYYMMDD 00:00 because that would be for the previous
            # day, so we get data from YYYYMMDD 00:00:01 ...
            parsed_start_time = parsed_start_time + timedelta(seconds=1)
        if len(str(args.end_time)) == 8:
            # ... and for the end_time, we set the end_time to (YYYYMMDD + 1) 00:00 because that will get us all the
            # data for YYYYMMDD.
            parsed_end_time = parsed_end_time + timedelta(days=1)

        args.start_time = parsed_start_time
        args.end_time = parsed_end_time

        # Convert the command line args to UTC since that's what's stored in the DB.
        if args.timezone == 'pacific':
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
    # station -> time_key -> data
    out_data_map = defaultdict(dict)

    # Put the data into bins
    for ele in data:
        dl_id = ele["station"]

        # Binning is tricky!  Data timestamped with time X is for sensor readings from time 'X - 1 hour' up until X.
        # Thus for daily binning for day YYYYMMDD, we take data from YYYYMMDD 01:00 to (YYYYMMDD + 1) 0:00.  We need
        # to make a similar adjustment for AM/PM binning, taking data from [1:00, 12:00] for AM and
        # [13:00, 0:00] for PM.
        if ele['time'].hour == 0:
            yesterday_dt = ele['time'] - timedelta(days=1)
            base_date = yesterday_dt.strftime("%Y%m%d")
        else:
            base_date = ele['time'].strftime("%Y%m%d")

        if bin_type == "daily":
            time_key = base_date
        else:
            if 1 <= ele['time'].hour <= 12:
                time_key = base_date + "-AM"
            else:
                time_key = base_date + "-PM"

        # If we've never seen this dl_id-time_key combo, initialize our data to '0.'
        if dl_id not in out_data_map or time_key not in out_data_map[dl_id]:
            out_data_map[dl_id][time_key] = dict()

            for k, v in ele.items():
                if k == 'station':
                    new_v = v
                elif k == 'time':
                    new_v = time_key
                else:
                    new_v = None

                out_data_map[dl_id][time_key][k] = new_v

        # Now add in the elements
        for k, v in ele.items():
            if BinningOp.is_binnable_col(k):
                old_value = out_data_map[dl_id][time_key][k]
                out_data_map[dl_id][time_key][k] = BinningOp.mix_in_value(old_value, k, v)

    # Turn the output dict into a list
    # todo Is there a more Pythonic way to do this?
    output_list = list()
    for dl_id, dl_data in out_data_map.items():
        for time_key, sensor_data in dl_data.items():
            for k, v in sensor_data.items():
                if BinningOp.is_binnable_col(k):
                    sensor_data[k] = BinningOp.complete_bin(k, v)
            output_list.append(sensor_data)

    return output_list


def main():
    args, dbinfo = configure_script()

    rp = ResultPrinter("," if args.print_csv else " ")

    # If we have a start time, we must want to get data, so let's do just that.
    if args.start_time is not None:
        sensor_str = ", ".join(map(lambda x: "M." + x, args.sensors))
        station_str = " OR ".join(map(lambda x: "DL.datalogger_char_id='{}'".format(x), args.stations))

        if args.timezone == 'pacific':
            time_query_str = "CONVERT_TZ(M.timecode, 'UTC', 'US/Pacific')"
        else:
            time_query_str = "M.timecode"

        query = "SELECT DL.datalogger_char_id AS 'station', {time_query_str} AS 'time', {sensor} " \
                "FROM weatherstations_datalogger DL " \
                "INNER JOIN weatherstations_measurement M " \
                "ON M.data_logger_id = DL.id " \
                "WHERE M.timecode>='{start_dt}' AND M.timecode<='{end_dt}' AND ({stations})".\
            format(time_query_str=time_query_str,
                   sensor=sensor_str,
                   start_dt=args.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                   end_dt=args.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                   stations=station_str)

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('station', 5)
            if args.station_id_type == 'mesowest':
                rp.add_column('mesowest_id', 7)
            rp.add_column('time', 18)
            for sensor in args.sensors:
                rp.add_column(sensor, 5)

            data = list(cursor.fetchall())
            if args.do_binning is not None:
                data = bin_data(args.do_binning, data)
            data.sort(key=lambda x: x['station'])

            if args.print_header:
                rp.print_header()
            for ele in data:
                if args.station_id_type == 'mesowest':
                    ele['mesowest_id'] = nwac.StationNameConversion.convert_aws_to_mesowest(ele['station'])
                if args.do_binning is None:
                    ele['time'] = ele['time'].strftime('%Y%m%d %H:%M:%S')
                rp.print_datum(ele)

    # If no stations are specified, get a list of all the stations
    elif args.stations is None or len(args.stations) == 0:

        query = "SELECT DL.id, DL.datalogger_name, DL.datalogger_char_id AS 'aws_id', WDS.title " \
                "FROM weatherstations_datalogger AS DL " \
                "LEFT JOIN weatherdata_station as WDS " \
                "ON DL.station_id = WDS.id"

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('id', 3)
            rp.add_column('aws_id', 5)
            rp.add_column('mesowest_id', 7)  # value injected to ele below
            rp.add_column('datalogger_name', 45)
            rp.add_column('title', None, 'station_title')

            data = list(cursor.fetchall())
            data.sort(key=lambda x: x['id'])
            if args.print_header:
                rp.print_header()
            for ele in data:
                mw_id = nwac.StationNameConversion.convert_aws_to_mesowest(ele['aws_id'])
                ele['mesowest_id'] = mw_id if mw_id is not None else '-'
                rp.print_datum(ele)
    # We have at least one station specified but no start and end date, so we must want a list of all the sensors at
    # those stations.
    else:
        query = "SELECT DL.datalogger_char_id AS 'station', T.sensortype_name, T.field_name " \
                "FROM weatherstations_datalogger DL " \
                "INNER JOIN weatherstations_sensor S ON S.data_logger_id = DL.id " \
                "INNER JOIN weatherstations_sensortype T ON S.sensor_type_id = T.id " \
                "WHERE "

        for i, station in enumerate(args.stations):
            if i >= 1:
                query += " OR "
            query += "DL.datalogger_char_id='{}'".format(station)

        cursor = process_query(dbinfo, args, query)

        if cursor is not None:
            rp.add_column('station', 5)
            if args.station_id_type == 'mesowest':
                rp.add_column('mesowest_id', 7)
            rp.add_column('sensortype_name', 25)
            rp.add_column('field_name', 25)

            data = list(cursor.fetchall())
            data.sort(key=lambda x: x['station'])
            if args.print_header:
                rp.print_header()
            for ele in data:
                if args.station_id_type == 'mesowest':
                    ele['mesowest_id'] = nwac.StationNameConversion.convert_aws_to_mesowest(ele['station'])
                rp.print_datum(ele)


# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
