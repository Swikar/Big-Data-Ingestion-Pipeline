import sys
import os
import os.path
import mysql.connector
from impala.dbapi import connect
import subprocess

import ingestionUtil

reload(sys)
sys.setdefaultencoding("UTF8")

print("Initializing Environment...")
P_FILE_ID = str(sys.argv[1]) if len(sys.argv) > 1 else None

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
    files = []
    # Generate a MySQL SELECT to obtain all external table metadata
    SQL_STR = "SELECT input_path FROM ext_table"
    #THIS LINE FOR TESTING ONLY:
    # SQL_STR += ' WHERE file_id LIKE "8%"'
    # SQL_STR += " WHERE file_id like '13%'"
    if(P_FILE_ID != None):
        SQL_STR += " WHERE file_id=" + P_FILE_ID
    cursor.execute(SQL_STR)
    for x in cursor:
        files.append(x)
    # print(files)
    # subprocess.call(["python", "hello.py"])
    for (input_path) in files:
        ip = input_path[0]
        sl = ip.split('/')
        pp = [x for x in sl]
        pp.insert(3, 'profiler')
        sl[3] = 'archive'
        ap = '/'.join(sl)
        sl[3] = 'secure'
        sp = '/'.join(sl)
        pp = '/'.join(pp)
        # print(sl)

        # print(pp)
        # print(ip)

        try:
            #WRITE HDFS MKDIR COMMANDS
            in_command = ["hdfs", "dfs", "-mkdir", "-p", ip]
            ar_command = ["hdfs", "dfs", "-mkdir", "-p", ap]
            pp_command = ["hdfs", "dfs", "-mkdir", "-p", pp]
            hdfs_commands = [in_command, ar_command, pp_command]
            for command in hdfs_commands:
                subprocess.call(command)
                print(' '.join(command))

            #WRITE EDGENODE MKDIR COMMANDS
            en_in = ['mkdir', '-p', ip]
            en_ar = ['mkdir', '-p', ap]
            en_pp = ['mkdir', '-p', pp]
            en_sp = ['mkdir', '-p', sp]
            en_commands = [en_in, en_ar, en_pp, en_sp]
            for command in en_commands:
                subprocess.call(command)
                print(' '.join(command))



        except Exception as e:
            print("Error: " + str(e) + "\n")
            log.close()
            sys.exit(-1)

    log.close()
    sys.exit(0)


except Exception as e:
    print("Error: " + str(e) + "\n")
    log.close()
    sys.exit(-1)
