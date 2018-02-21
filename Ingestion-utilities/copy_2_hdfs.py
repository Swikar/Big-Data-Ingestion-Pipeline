#!/usr/bin/python

# (c) Copyright IBM 2017

import sys
import subprocess
import os
import time
import ingestionUtil

################################################################################
#
#      Create Log File and Declare Constants
#
################################################################################

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

P_FROM_FILE=str(sys.argv[1])
P_TO_FILE=str(sys.argv[2])

print("Copying file: " + P_FROM_FILE + " to HDFS: " + P_TO_FILE)

x=subprocess.call("hadoop fs -put " + P_FROM_FILE + " " + P_TO_FILE, shell=1)

if x == 0:
	print("Copy Successful")
	log.close()
else:
	print("Error: " + str(x) + " Copy Failed")
	log.close()

exit(x)
