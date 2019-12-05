#!/usr/local/bin/python3

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------
# TODO organize these
import datetime
from datetime import date
import seaborn as sns
import copy
# from datetime import date_range
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import metrics
import os
import os.path
import configparser

# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURATION and INITIALIZATION
# ---------------------------------------------------------------------------------------------------------------------

# Read in the config file
config = configparser.ConfigParser()
config.read('config.ini')
PROJ_DIR = config['DEFAULT']['PROJECT_DIR']

# pandas settings for outputting numerical data
# TODO Robert, why are these set this way?  Does this make the output play nicely with Jupyter?
# pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format

# Close all previous plots
plt.close("all")

# TODO Make this a command line argument
MAKE_PLOTS = False

# TODO Make this a command line argument
# Check to make sure the directory exists; if not, make it.
OUTPUT_DIR = os.path.join(PROJ_DIR, 'outdir')
if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)

# TODO Make this a command line argument
date_rng = pd.date_range(start='12/11/2018', end='4/30/2019', freq='D')
# date_rng = pd.date_range(start='11/25/2018', end='5/13/2019', freq='D')

# Get the path to the data file
# TODO Make this a command line argument
DATA_FILE = os.path.join(PROJ_DIR, 'BiasCorrectionData_new.csv')

# TODO Auto-detect this?
COLUMNS = ['HUR2', 'MTB2', 'WAP2', 'STV2', 'SNO2', 'LVN2', 'MIS2', 'CMT2', 'PAR2', 'WHP2', 'TML2', 'MHM2',            'HUR3', 'MTB3', 'WAP3', 'STV3', 'SNO3', 'LVN3', 'MIS3', 'CMT3', 'PAR3', 'WHP3', 'TML3', 'MHM3']
NAMES = ['HUR', 'MTB', 'WAP', 'STV', 'SNO', 'LVN', 'MIS', 'CMT', 'PAR', 'WHP', 'TML', 'MHM']
# names = ['STV']

# ---------------------------------------------------------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------------------------------------------------------

def plotFigure(data_plot, file_name, order):
    fig = plt.figure(order, figsize=(9, 6))
    ax = fig.add_subplot(111)
    bp = ax.boxplot(data_plot)
    fig.savefig(file_name, bbox_inches='tight')
    plt.close()

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------
# TODO Remove various commented out scratch code.

# Read in the data file
df = pd.read_csv(DATA_FILE)
df.columns = df.columns.str.strip()
df = df.iloc[16:157, :]
df['Date'] = date_rng
df = df.set_index('Date')

df.drop(COLUMNS, inplace=True, axis=1)

for name in NAMES:
    tau = 30
    df2 = copy.deepcopy(df.filter(regex=name))
    df2[name + '_CF'] = 0
    df2[name + '_BC'] = 0
    bc_fcst = df2[name + '_BC']
    df2[name + '_Raw_Bias'] = 0
    raw_bias = df2[name + '_Raw_Bias']
    df2[name + '_BC_Bias'] = 0
    bc_bias = df2[name + '_BC_Bias']
    obs = df2[name + '1']
    fcst = df2[name + '4']
    cf = df2[name + '_CF']
    cf.iloc[0] = 1
    df3 = copy.deepcopy(df.filter(regex=name).dropna(axis=0, how='any'))
    df3[name + '_CF'] = 0
    cf_drop = df3[name + '_CF']
    df3[name + '_BC'] = 0
    bc_fcst_drop = df3[name + '_BC']
    df3[name + '_Raw_Bias'] = 0
    raw_bias_drop = df3[name + '_Raw_Bias']
    df3[name + '_BC_Bias'] = 0
    bc_bias_drop = df3[name + '_BC_Bias']
    obs_drop = df2[name + '1']
    fcst_drop = df2[name + '4']
    cf_drop = df2[name + '_CF']
    cf.iloc[0] = 1

    for i in range(len(df2) - 1):
        if (obs.iloc[i] <= 0.01 or np.isnan(obs.iloc[i]) == True or np.isnan(fcst.iloc[i]) == True):
            cf.iloc[i + 1] = cf.iloc[i]
        else:
            cf.iloc[i + 1] = ((tau - 1) / tau) * cf.iloc[i] + (1 / tau) * (fcst.iloc[i] / obs.iloc[i])
            # code to avoid large jumps in cf
            if (abs((cf.iloc[i + 1] / cf.iloc[i])) > 1.5 and (fcst.iloc[i] + obs.iloc[i]) < 1):
                cf.iloc[i + 1] = cf.iloc[i] + (cf.iloc[i + 1] - cf.iloc[i]) / (cf.iloc[i + 1] + cf.iloc[i])
        bc_fcst.iloc[i] = fcst.iloc[i] / cf.iloc[i]
        bc_bias.iloc[i] = bc_fcst.iloc[i] - obs.iloc[i]
        raw_bias.iloc[i] = fcst.iloc[i] - obs.iloc[i]
        #         print("date is " + str(df.index.date[i]) + "; fcst is " + str(round(fcst.iloc[i],2)) + "; obs is " + str(round(obs.iloc[i],2)) + "; cf is " + str(round(cf.iloc[i],2)) + \
        #                 "; fcst_bc is " + str(round(bc_fcst.iloc[i],2)))
        print("date is " + str(df.index.date[i]) + "; fcst is " + str(round(fcst.iloc[i], 2)) + "; obs is " + str(
            round(obs.iloc[i], 2)) + "; cf is " + str(round(cf.iloc[i], 2)) + \
              "; bc_fcst is " + str(round(bc_fcst.iloc[i], 2)))

        a = open((OUTPUT_DIR + '/' + name + '_precip.txt'), 'w')
        a.write(str(df3))
        a.close()

    # Skip plot generation if so specified.
    if not MAKE_PLOTS:
        continue

    fig = plt.figure(figsize=(16, 16))
    raw_bias.plot(figsize=(20, 10), fontsize=20, color="green")
    bc_bias.plot(figsize=(20, 10), fontsize=20, color="red")
    #     bc_fcst.plot(figsize=(20,10), fontsize=20, color="blue")
    #     fcst.plot(figsize=(20,10), fontsize=20, color="magenta")
    #     obs.plot(figsize=(20,10), fontsize=20, color="orange")
    cf.plot(figsize=(20, 10), fontsize=20, color="black")
    plt.xlabel('Month', fontsize=20)
    plt.ylabel('Precip Bias (")', fontsize=20)
    plt.figtext(0.35, 0.85, name + " Raw 1.33-km WRF MAE = " + str(
        round(metrics.mean_absolute_error(df2[name + '4'], df2[name + '1']), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)
    plt.figtext(0.35, 0.8, name + " Raw 1.33-km WRF MSE = " + str(
        round(metrics.mean_squared_error(df2[name + '4'], df2[name + '1']), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)
    plt.figtext(0.35, 0.75, name + " Raw 1.33-km WRF RMSE = " + str(
        round(np.sqrt(metrics.mean_absolute_error(df2[name + '4'], df2[name + '1'])), 3)),
                wrap=True, horizontalalignment='center', fontsize=16)
    plt.figtext(0.65, 0.85, name + " BC 1.33-km WRF MAE = " + str(
        round(metrics.mean_absolute_error(df2[name + '_BC'], df2[name + '1']), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)
    plt.figtext(0.35, 0.15, name + " Mean Raw 1.33-km WRF Bias = " + str(
        round(raw_bias.loc['2018-12-25 00:00:00':'2019-05-13 00:00:00'].mean(), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)
    plt.figtext(0.65, 0.8, name + " BC 1.33-km WRF MSE = " + str(
        round(metrics.mean_squared_error(df2[name + '_BC'], df2[name + '1']), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)
    plt.figtext(0.65, 0.75, name + " BC 1.33-km WRF RMSE = " + str(
        round(np.sqrt(metrics.mean_absolute_error(df2[name + '_BC'], df2[name + '1'])), 3)),
                wrap=True, horizontalalignment='center', fontsize=16)
    plt.figtext(0.65, 0.15, name + " Mean BC WRF 1.33-km Bias = " + str(
        round(bc_bias.loc['2018-12-25 00:00:00':'2019-05-13 00:00:00'].mean(), 3)), wrap=True,
                horizontalalignment='center', fontsize=16)

    plt.legend(fontsize=16)
    plt.title("STN = " + name + " FH12-36 Forecast Comparison: 1.33-km WRF and BC WRF Precip Bias", fontsize=20)
    plt.show()
    fig.savefig(PROJ_DIR + '/STN=' + name + '_WRF_vs_BCWRF.png', dpi=180)
    plt.close()

print(OUTPUT_DIR)
print(DATA_FILE.strip())
print(DATA_FILE)
print(cf.iloc[i] + (cf.iloc[i + 1] - cf.iloc[i]) / (cf.iloc[i + 1] + cf.iloc[i]))
print(cf.iloc[i])
cf.iloc[i + 1]
for x2 in range(1, 101, 1):
    for y2 in range(1, 101, 1):
        x = x2 / 100
        y = y2 / 100
        ycorr = x + (y - x) / (y + x)
        print(x, y, ycorr)
        
get_ipython().system('jupyter nbconvert --to script BiasCorrection_corrected.ipynb')


