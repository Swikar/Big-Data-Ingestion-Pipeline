#!/usr/bin/python


import sys
import subprocess
import os
import time
import random
import ingestionUtil
from collections import defaultdict

################################################################################
#
#      Declare Constants
#
################################################################################

P_FILE=str(sys.argv[1])
P_HEADER=str(sys.argv[2])

################################################################################
#
#     Execute Code
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

print("Counting records present in file...")
lines = 0
for line in open(P_FILE):
        lines += 1

print("Reading record count in header file...")
file = open(P_HEADER, 'r')
rec_cnt = int(file.read())

print("Comparing record count with header file...")
if lines == rec_cnt:
        print("Success")
        sys.exit(0)
else:
        print("Fail")
        sys.exit(-1)
