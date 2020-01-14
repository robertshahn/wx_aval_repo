#!/usr/local/bin/python3

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------
import argparse
import configparser
import inspect
import os
import subprocess
import sys

from datetime import datetime

# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURATION and INITIALIZATION
# ---------------------------------------------------------------------------------------------------------------------
# no 'TUM' for now because that isn't a unique AWS ID
DEFAULT_STATIONS = ['HUR', 'MTB', 'WAS', 'STS', 'SSM', 'MSM', 'CMT', 'PVC', 'WPS', 'TML', 'MHL']

SCRIPT_DIR = "/".join(os.path.abspath(inspect.getsourcefile(lambda:0)).split("/")[:-1])
DEFAULT_CONFIG_FILE = SCRIPT_DIR + "/daily_bc_gen.config"

KLEIO_STDERR_FILE = "kleio.stderr"
KLEIO_STDOUT_FILE = "kleio.stdout"

# ---------------------------------------------------------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------------------------------------------------------
def configure_script():
    parser = argparse.ArgumentParser(description='Configure and run gen_bc.py to generate a new set of bias '
                                     'correction factors.')

    parser.add_argument('-s', action='store',
                        help="Start date specified as 'YYYYMMDD'.",
                        required=True,
                        dest='start_date')
    parser.add_argument('-e', action='store',
                        help="End date specified as 'YYYYMMDD'.  Analysis is run up to and including the end date. "
                             "If a start date is specified ('-s') but this argument is omitted, we set the end date to "
                             "the start date.",
                        dest='end_date')

    parser.add_argument('-o', action='store',
                        help="Output directory.  If this path does not exist, one is created.",
                        required=True,
                        dest='out_dir')

    parser.add_argument('-L', action='store',
                        help="Space-separated list of stations for which to get data.  Stations are specified via "
                             "their AWS ID.  Defaults to '{stations}'.".format(
                            stations=" ".join(DEFAULT_STATIONS)),
                        nargs="*",
                        default=DEFAULT_STATIONS,
                        dest='stations')

    parser.add_argument('-c', action='store',
                        help="Config file.  Default value: {config}".format(config=DEFAULT_CONFIG_FILE),
                        default=DEFAULT_CONFIG_FILE,
                        dest='config_file')

    args = parser.parse_args()

    # Make sure the specified start and end time are dates in YYYYMMDD format.
    if args.end_date is None:
        args.end_date = args.start_date
    try:
        # We don't need to store the values, we just need to make sure they parse.
        date_format = "%Y%m%d"
        datetime.strptime(args.start_date, date_format)
        datetime.strptime(args.end_date, date_format)
    except ValueError:
        sys.stderr.write("Invalid start date or end date:  {start} {end}.\n"
                         "These must be specified as YYYYMMDD.\n".format(
            start=args.start_date,
            end=args.end_date))

    # Create the output directory if it doesn't exist.  Warn if it isn't empty.
    if not os.path.exists(args.out_dir):
        os.mkdir(args.out_dir)
    if os.listdir(args.out_dir):
        sys.stderr.write("Warning!  Output directory ({out}) is not empty.\n".format(
            out=args.out_dir))

    # Read in the config file
    config = configparser.ConfigParser()
    config.read(args.config_file)
    args.kleio_path = config['DEFAULT']['KLEIO_PATH']

    return args

def main():
    args = configure_script()

    # -----------------------------------------------------------------------------------------------------------------
    # 1) Run kleio and handle output to get observations.

    # Call kleio
    kleio_command = "{kleio_path} -s {start} -e {end} --bin ampm -L {stations} -S precipitation".format(
        kleio_path=args.kleio_path,
        start=args.start_date,
        end=args.end_date,
        stations=" ".join(args.stations))
    kleio_pipes = subprocess.Popen(kleio_command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    k_stdout, k_stderr = kleio_pipes.communicate()

    # Always print the stderr to this script's stderr and a log file.
    if len(k_stderr) != 0:
        #todo Write to these files via a Logger?
        f = open(os.path.join(args.out_dir, KLEIO_STDERR_FILE), "w")
        for line in k_stderr.splitlines():
            f.write(line + "\n")
            sys.stderr.write("kleio.py stderr >>>> {line}\n".format(line=line))
        f.close()
    if kleio_pipes.returncode != 0:
        sys.stderr.write("Error running kleio command: {cmd}\n".format(cmd=kleio_command))
        exit(1)

    # If we're here, we don't hae an error, so load in the output values.
    obs_data = dict()  # date -> station -> value
    f = open(os.path.join(args.out_dir, KLEIO_STDOUT_FILE), "w")
    for line in k_stdout.splitlines():
        f.write(line + "\n")

        logger, date_str, value = line.split()
        if date_str not in obs_data:
            obs_data[date_str] = dict()
        obs_data[date_str][logger] = value

    f.close()

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
