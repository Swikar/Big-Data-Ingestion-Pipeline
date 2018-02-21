#!/usr/bin/python


import sys
import subprocess
import os

import mysql.connector
from impala.dbapi import connect

import ingestionUtil

# Read Parameters
print("Initializing Environment...")
P_FILE_ID = str(sys.argv[1]) if len(sys.argv) > 1 else None

if P_FILE_ID is None:
    print("Parameter Error: File ID [1] is not specified")
    sys.exit(-1)

LOGFILE = ingestionUtil.log_path(sys.argv[0])

#Create Standard Logging Class to write Stdout to screen and LOGFILE
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(LOGFILE, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

sys.stdout = Logger()

print("Logging to file: " + LOGFILE)
log = open(LOGFILE, 'w')

print("File ID: " + P_FILE_ID + "\n")

# Connect to MySQL Metadata Repository
try:
    print("Connecting to MySQL server... \n")
    cnx = ingestionUtil.connect_to_mysql()
    cursor = cnx.cursor()
    print("Connection to MySQL is successful \n")

except Exception as e:
    print("Connection to MySQL Failed: " + str(e) + "\n")
    log.close()
    sys.exit(-1)


try:
    # Generate a MySQL SELECT to obtain all external table metadata
    SQL_STR = "SELECT file_id, file_name, ext_table_name, delim, input_path, hive_db FROM ext_table"
    SQL_STR += " WHERE file_id=" + P_FILE_ID
    cursor.execute(SQL_STR)
    for (file_id, file_name, ext_table_name, delim, input_path, hive_db) in cursor:
        f_id = file_id
        fn = file_name
        etn = ext_table_name
        d = delim
        ip = input_path
        hdb = hive_db

    # Start the DDL with CREATE EXTERNAL TABLE
    print("Generating DDL... \n")
    DDL_STR = "CREATE EXTERNAL TABLE " + hdb + "." + etn + " ( "

    # Generate a MySQL SELECT to obtain all external table column metadata
    SQL_STR = "SELECT col_name, d_type FROM ext_table_def "
    SQL_STR += " WHERE file_id=" + P_FILE_ID
    SQL_STR += " ORDER BY col_order"
    cursor.execute(SQL_STR)
    # Add columns and corresponding data type onto DDL
    for (col_name, d_type) in cursor:
        DDL_STR += col_name + " " + "string" + ", "

    cnx.close()
    print("MySQL Connection Closed \n")

    # Finish DDL
    DDL_STR = DDL_STR[:-2]
    DDL_STR += ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + d + "' "
    DDL_STR += "LINES TERMINATED BY '\\n' STORED AS TEXTFILE "
    DDL_STR += "LOCATION '" + ip + "'"
    #added to handle non-ASCII delimiters
    DDL_STR = DDL_STR.encode('utf-8')
    print("DDL Creation Successful \n")

    # Generate a DROP string to drop the table if it exists
    DROP_STR = "DROP TABLE IF EXISTS " + hdb + "." + etn

    print("Connecting to Hive...\n")
    cnx = ingestionUtil.connect_to_hive()
    cursor = cnx.cursor()
    cursor.execute('set role admin')
    print("Connection to Hive is successful \n")

    '''
    DROP mode, to be implemented if DROP is needed?
    Currently DROP is always executed
    '''
    print("Executing DROP... \n")
    print(DROP_STR + " \n")
    cursor.execute(DROP_STR)

    print("Executing DDL... \n")
    print(DDL_STR + " \n")
    cursor.execute(DDL_STR)

    cnx.close()
    print("Hive Connection Closed \n")
    print("External Table " + P_FILE_ID + " Created \n")
    log.close()
    sys.exit(0)


except Exception as e:
    print("Error: " + str(e) + "\n")
    log.close()
    sys.exit(-1)
