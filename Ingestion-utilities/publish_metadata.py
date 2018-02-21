#!/usr/bin/python


#Import Libraries

import mysql.connector
import sys
import csv
import subprocess
import os
import time
import ConfigParser
import ingestionUtil
#####################################################################
#
#       Create Log and Declare Constants
#
#####################################################################

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

print("Initializing Environment...")
P_META_FILE = str(sys.argv[1])
P_META_TABLE = str(sys.argv[2])
P_CONFIG_FILE = ingestionUtil.CONFIG_FILE_PATH
print(P_CONFIG_FILE)
print("Metadata File: " + P_META_FILE)

#####################################################################
#
#       Connect to Metadata Repository and Convert File
#
#####################################################################

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
    cursor = cnx.cursor()

    print("Assembling metadata insert...")
    INSERT_STR = """LOAD DATA LOCAL INFILE """ + """'""" + P_META_FILE + """'""" + """ INTO TABLE """ + P_META_TABLE + """ FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES"""
    print INSERT_STR
    cursor.execute(INSERT_STR)
    cnx.commit()
    cursor.close()
    print("Metadata insert completed for file: " + P_META_FILE)
    log.close()
    sys.exit(0)

except Exception as e:
	print(e)
	log.close()
	sys.exit(-1)
