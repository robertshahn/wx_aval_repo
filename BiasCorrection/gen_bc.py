#!/usr/local/bin/python3

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------
import argparse
import configparser
import os.path
import sys
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics

# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURATION and INITIALIZATION
# ---------------------------------------------------------------------------------------------------------------------

# pandas settings for outputting numerical data
# These allow us to just an entire DataFrame to a file.
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format

# Close all previous plots
plt.close("all")

# TODO Useful for testing, but perhaps remove this for prod.
DEFAULT_CSV_NAME = 'BiasCorrectionData.csv'
DEFAULT_START_DATE = 20181211
DEFAULT_END_DATE = 20190430
DEFAULT_STATIONS = ['HUR', 'MTB', 'WAP', 'STV', 'SNO', 'LVN', 'MIS', 'CMT', 'PAR', 'WHP', 'TML', 'MHM']
TAU = 30
#TODO Robert, will the column headers always be SSSD where SSS is the station identifier and D is [1,4]?
SUFFIXES_TO_DROP = ['2', '3']

# ---------------------------------------------------------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------------------------------------------------------

def read_csv_data(file_name, start_date, end_date):
    # read in the CSV data
    dataframe = pd.read_csv(file_name)

    # filter based on date
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    dataframe = dataframe.set_index('Date')
    dataframe= dataframe.loc[start_date:end_date]

    # Figure out which columns we'll be dropping
    cols_to_drop = []
    for suffix in SUFFIXES_TO_DROP:
        cols_to_drop.extend(dataframe.filter(regex="{}$".format(suffix), axis='columns').columns)
    dataframe.drop(cols_to_drop, inplace=True, axis='columns')

    return dataframe

def prep_station_dataframe(dataframe, name):
    # Get a copy of the data so we can easily output it
    stat_df = dataframe.filter(regex=name).copy(deep=True)

    # Rename the observation and forecast columns.
    # Tricky!!  Do this first because any references to columns within the DataFrame will change with
    # the call to rename().
    obs_lbl = name + '_OBS'
    fcst_lbl = name + '_FCST'
    stat_df.rename(columns={name + '1': obs_lbl, name + '4': fcst_lbl}, inplace=True)
    obs = stat_df[obs_lbl]
    fcst = stat_df[fcst_lbl]

    # Set up the extra columns we need for our math
    cf_lbl = name + '_CF'
    stat_df[cf_lbl] = 0.0
    cf = stat_df[cf_lbl]

    bc_lbl = name + '_BC'
    stat_df[bc_lbl] = 0.0
    bc_fcst = stat_df[bc_lbl]

    raw_bias_lbl = name + '_Raw_Bias'
    stat_df[raw_bias_lbl] = 0.0
    raw_bias = stat_df[raw_bias_lbl]

    bc_bias_lbl = name + '_BC_Bias'
    stat_df[bc_bias_lbl] = 0.0
    bc_bias = stat_df[bc_bias_lbl]

    return stat_df, obs, fcst, cf, bc_fcst, raw_bias, bc_bias

def gen_station_cf(name, stat_df, obs, fcst, cf, bc_fcst, raw_bias, bc_bias, args):
    cf.iat[0] = 1.0
    for i in range(len(stat_df) - 1):
        # Get some handy nicknames to make the code more readable...
        obs_today = obs.iat[i]
        fcst_today = fcst.iat[i]
        cf_today = cf.iat[i]

        # If the observation is (nearly) zero or we have an error flag for the observation or forecast,
        # set tomorrow's correction factor to today's.
        if obs_today <= 0.01 or np.isnan(obs_today) or np.isnan(fcst_today):
            cf.iat[i + 1] = cf_today
        else:
            # Update the correction factor for tomorrow.
            #
            # This formula is based on "Reliable probabilistic forecasts from an ensemble reservoir inflow
            # forecasting system" by Bourdin, Nipen, and Stull:
            # https://agupubs.onlinelibrary.wiley.com/doi/pdf/10.1002/2014WR015462.
            #
            # This formula is equivalent to a geometric series of the form:
            # cf_i = 1 / TAU * SUM(k=[0,infinity))(r^k * a_i-k-1) where
            # r = (TAU - 1) / TAU and a_x = fcst_x / obs_x
            cf.iat[i + 1] = (((TAU - 1) / TAU) * cf_today) + ((1 / TAU) * (fcst_today / obs_today))

            # Avoid large jumps in the correction factor:
            # If the CF increases by more than 50% and the sum of the forecast and observed precip is less than 1, then
            # normalize (currently there is an error in this).
            # TODO make '1.5' a configurable global variable
            # TODO this only prevents increases in the CF, what about decreases?
            cf_tmrw = cf.iat[i + 1]
            if (cf_tmrw / cf_today > 1.5 and (fcst_today + obs_today) < 1):
                # TODO fix this normalization so it does something mathematically sound (a logarithm?)
                cf.iat[i + 1] = cf_today + (cf_tmrw - cf_today) / (cf_tmrw + cf_today)

        # Update the bias-corrected forecast and measures of bias
        bc_fcst.iat[i] = fcst_today / cf_today
        bc_bias.iat[i] = bc_fcst.iat[i] - obs_today
        raw_bias.iat[i] = fcst_today - obs_today

        # print out a message
        if not args.silence:
            # TODO format date as YYYYMMDD?
            print("{date} {station} {fcst} {obs} {cf} {bc_fcst}".format(
                date=stat_df.index.date[i],
                station=name,
                fcst=str(round(fcst_today, 2)),
                obs=str(round(obs_today, 2)),
                cf=str(round(cf_today, 2)),
                bc_fcst=str(round(bc_fcst.iat[i], 2))
            ))

def add_plot_text(plt, x, y, text):
    plt.figtext(x, y, text, wrap=True, horizontalalignment='center', fontsize=16)

def make_plots(outdir, name, obs, fcst, cf, bc_fcst, raw_bias, bc_bias, args):
    fig = plt.figure(figsize=(16, 16))

    # FIXME make what gets printed a command line argument
    raw_bias.plot(figsize=(20, 10), fontsize=20, color="green")
    bc_bias.plot(figsize=(20, 10), fontsize=20, color="red")
    #     bc_fcst.plot(figsize=(20,10), fontsize=20, color="blue")
    #     fcst.plot(figsize=(20,10), fontsize=20, color="magenta")
    #     obs.plot(figsize=(20,10), fontsize=20, color="orange")
    cf.plot(figsize=(20, 10), fontsize=20, color="black")

    plt.xlabel('Month', fontsize=20)
    plt.ylabel('Precip Bias (")', fontsize=20)

    # FIXME Handle exception when NaN's appear in data munched by metrics methods
    add_plot_text(plt, 0.35, 0.85,
                  name + " Raw 1.33-km WRF MAE = " + str(round(metrics.mean_absolute_error(fcst, obs), 3)))
    add_plot_text(plt, 0.35, 0.8,
                  name + " Raw 1.33-km WRF MSE = " + str(round(metrics.mean_squared_error(fcst, obs), 3)))
    add_plot_text(plt, 0.35, 0.75,
                  name + " Raw 1.33-km WRF RMSE = " + str(round(np.sqrt(metrics.mean_absolute_error(fcst, obs)), 3)))
    add_plot_text(plt, 0.65, 0.85,
                  name + " BC 1.33-km WRF MAE = " + str(round(metrics.mean_absolute_error(bc_fcst, obs), 3)))
    # FIXME get rid of the magic range
    add_plot_text(plt, 0.35, 0.15,
                  name + " Mean Raw 1.33-km WRF Bias = " +
                  str(round(raw_bias.loc['2018-12-25 00:00:00':'2019-05-13 00:00:00'].mean(), 3)))
    add_plot_text(plt, 0.65, 0.8,
                  name + " BC 1.33-km WRF MSE = " + str(round(metrics.mean_squared_error(bc_fcst, obs), 3)))
    add_plot_text(plt, 0.65, 0.75,
                  name + " BC 1.33-km WRF RMSE = " + str(round(np.sqrt(metrics.mean_absolute_error(bc_fcst, obs)), 3)))
    add_plot_text(plt, 0.65, 0.15,
                  name + " Mean BC WRF 1.33-km Bias = " +
                  str(round(bc_bias.loc['2018-12-25 00:00:00':'2019-05-13 00:00:00'].mean(), 3)))

    plt.legend(fontsize=16)
    plt.title("STN = " + name + " FH12-36 Forecast Comparison: 1.33-km WRF and BC WRF Precip Bias", fontsize=20)

    if not args.silence:
        plt.show()

    # save the plot to the output directory
    fig.savefig(outdir + '/' + name + '--WRF_vs_BCWRF.png', dpi=180)
    plt.close()

def configure_script():
    # Read in the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    proj_dir = config['DEFAULT']['PROJECT_DIR']

    parser = argparse.ArgumentParser(description='Generate correction factor for NWAC wx data.')

    parser.add_argument('-s', action='store', type=int,
                        help="start date, defaults to {}".format(DEFAULT_START_DATE),
                        default=DEFAULT_START_DATE,
                        dest='start')
    parser.add_argument('-e', action='store', type=int,
                        help="end date, defaults to {}".format(DEFAULT_END_DATE),
                        default=DEFAULT_END_DATE,
                        dest='end')
    parser.add_argument('-i', '--input', action='store',
                        help="Path to input CSV file.",
                        default=os.path.join(proj_dir, DEFAULT_CSV_NAME),
                        dest='input_file_path')
    parser.add_argument('-o', '--output', action='store',
                        help="Path to output directory.",
                        default=os.path.join(proj_dir, 'outdir'),
                        dest='outdir')
    parser.add_argument('-S', action='store_true',
                        help="Silence output, i.e., daily correction factor calculations per station and plots " +
                        "(if we're generating plots).  Otherwise, daily text output is: " +
                        "<date> <station name> <fcst> <obs> <cf> <fcst w/ cf>",
                        dest='silence')
    parser.add_argument('-p', action='store_true',
                        help="Generate plots for data",
                        dest='make_plots')
    parser.add_argument('stations', metavar='S', action='store',
                        help="Wx stations for which to conduct analysis.  Must match CSV headers.  Possible values: " +
                        " ".join(DEFAULT_STATIONS) + ".",
                        nargs='*')

    args = parser.parse_args()

    # Get our state and end datetimes
    args.start = dt.datetime.strptime(str(args.start), "%Y%m%d").strftime("%Y-%m-%d")
    args.end = dt.datetime.strptime(str(args.end), "%Y%m%d").strftime("%Y-%m-%d")
    if args.start >= args.end:
        sys.stderr.write("Start must be before end.  You specified: {start} to {end}\n".format(
            start=args.start,
            end=args.end
        ))
        exit(1)

    # Check to make sure the output directory exists; if not, make it.
    if not os.path.exists(args.outdir):
        os.mkdir(args.outdir)

    # Get the stations for which we'll do the analysis.
    if len(args.stations) == 0:
        args.stations = DEFAULT_STATIONS
    else:
        input_stations = set(args.stations)
        # Make sure we got station names that we know about
        residual_stations = input_stations - set(DEFAULT_STATIONS)
        if len(residual_stations) != 0:
            sys.stderr.write("Invalid station names specified: {}\n".format(" ".join(residual_stations)))
            exit(1)

    return args

def main():
    args = configure_script()

    # Read in the data file, selecting the subset of the data we'd like
    dataframe = read_csv_data(args.input_file_path, args.start, args.end)

    for station_name in args.stations:
        # Get a copy of the station data we'll be editing
        stat_df, obs, fcst, cf, bc_fcst, raw_bias, bc_bias = \
            prep_station_dataframe(dataframe, station_name)

        # Generate the correction factor for this station
        gen_station_cf(station_name, stat_df, obs, fcst, cf, bc_fcst, raw_bias, bc_bias, args)

        # Write the bias-corrected forecast and other measures to a file, i.e., dump stat_df
        # TODO make this optional?
        a = open(os.path.join(args.outdir, station_name + '_precip.txt'), 'w')
        a.write(str(stat_df))
        a.close()

        # Make plots if so specified.
        if args.make_plots:
            make_plots(args.outdir, station_name, obs, fcst, cf, bc_fcst, raw_bias, bc_bias, args)

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
