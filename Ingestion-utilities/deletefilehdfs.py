#!/usr/bin/python

# (c) Copyright IBM 2017

import sys
import subprocess
import os
import time
import ingestionUtil

################################################################################
#
#      Declare Constants
#
################################################################################

print("Initializing Environment...")

DELETE_FILE=str(sys.argv[1])

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

print("Deleting file from HDFS: " + DELETE_FILE )

x=subprocess.call("hadoop fs -rm " + DELETE_FILE, shell=1)

if x == 0:
	print("Delete Successful")
	log.close()
else:
	print("Error: " + str(x) + " Delete Failed")
	log.close()

exit(x)
