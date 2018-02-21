#!/usr/bin/python

# (c) Copyright IBM 2017

# compare external table row count against .touch file

import mysql.connector
import sys
import subprocess
import os
import time
import random
import ConfigParser
from collections import defaultdict
from impala.dbapi import connect
import ingestionUtil

################################################################################
#
#      Create Log File and Declare Constants
#
################################################################################

# Read Parameters
print("Initializing Environment...")
P_FILE_ID = str(sys.argv[1]) if len(sys.argv) > 1 else None
if P_FILE_ID is None:
    print("Parameter Error: File ID [1] is not specified")
    sys.exit(-1)

TOUCH_FILE = str(sys.argv[2])
TOUCH_FILE = TOUCH_FILE.rstrip('/')
P_EXECUTION=str(sys.argv[3]) if len(sys.argv) > 3 else 'mr'
P_TABLE_COUNT=""

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

################################################################################
#
#     Execute Table and File Count Comparison
#
################################################################################


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

SQL_STR = "SELECT ext_table_name, hive_db from ext_table"
SQL_STR += " WHERE file_id=" + P_FILE_ID

cursor.execute(SQL_STR)
for(ext_table, hive_db) in cursor:
    ext_table = ext_table
    hdb = hive_db

P_TABLE_COUNT = "select count(*) from " + hdb + "." + ext_table



# Connect to Hive
try:
    print("Connecting to Hive...\n")
    cnx = ingestionUtil.connect_to_hive()
    cursor = cnx.cursor()
    print("Connection to Hive is successful \n")
    cursor.execute("set hive.execution.engine=" + P_EXECUTION)
    print("Executing external table count query...")
    print P_TABLE_COUNT
    cursor.execute(P_TABLE_COUNT)
    count = cursor.fetchone()
    rec_cnt = str(count)
    rec_cnt = rec_cnt.replace("(","")
    rec_cnt = rec_cnt.replace(")","")
    rec_cnt = rec_cnt.replace(",","")

    rec_cnt = int(rec_cnt)

except Exception as e:
	print(e)
	sys.exit(-1)


# open .touch file
#number_to_read = ''
try:
    file_to_read = open(TOUCH_FILE, 'r')
    number_to_read = file_to_read.read()
    number_to_read = int(number_to_read.strip())
    # print(repr(number_to_read))
    # print (type(rec_cnt))
except IOError:
    print "Error: can't find file or read data"

print("Comparing external table and file counts...")
if number_to_read == rec_cnt:
        print("Success")
        log.close()
        sys.exit(0)
else:
        print("Fail")
        log.close()
        sys.exit(-1)
