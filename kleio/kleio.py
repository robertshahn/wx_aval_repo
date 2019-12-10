#!/usr/local/bin/python3

# ---------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------------------------------------------------

import configparser
import argparse
import pymysql

# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURATION and INITIALIZATION
# ---------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------
# METHODS
# ---------------------------------------------------------------------------------------------------------------------

def main():
    # Read in the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    DB_HOSTNAME = config['DEFAULT']['DB_HOSTNAME']
    DB_USERNAME = config['DEFAULT']['DB_USERNAME']
    DB_PASSWORD = config['DEFAULT']['DB_PASSWORD']
    DB_NAME = config['DEFAULT']['DB_NAME']

    # Open database connection
    db = pymysql.connect(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_NAME)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("SELECT VERSION()")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print ("Database version : %s " % data)

    # disconnect from server
    db.close()

# ---------------------------------------------------------------------------------------------------------------------
# SCRIPT BODY
# ---------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
