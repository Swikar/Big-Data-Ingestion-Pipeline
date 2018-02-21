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
P_TABLE_ID = str(sys.argv[1]) if len(sys.argv) > 1 else None

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
subprocess.call(['alias', 'python=/usr/bin/python'])
try:
    files = []
    # Generate a MySQL SELECT to obtain all external table metadata
    SQL_STR = "SELECT table_id, table_name FROM target_table t1"
    if(P_TABLE_ID != None):
        SQL_STR += " WHERE table_id=" + P_TABLE_ID
    cursor.execute(SQL_STR)
    for x in cursor:
        files.append(x)
    # print(files)
    # subprocess.call(["python", "hello.py"])
    script = []
    for (table_id, table_name) in files:
        t_id = table_id
        tn = table_name

        try:
            #GET SUMS FROM RAW DATA FILE
            #command = ["hdfs", "dfs", "-mkdir", "-p", ip]
            #subprocess.call(command)

            command = ["python", "create_view.py", str(t_id)+'1']
            print(' '.join(command))
            script.append(' '.join(command))
            subprocess.call(['whoami'])
            subprocess.call(['which', 'python'])
            subprocess.call(command)


            #command = ["python", "create_ext_table.py", str(f_id)]
            #print(' '.join(command))
            #subprocess.call(command)


        except Exception as e:
            print("Error: " + str(e) + "\n")
            log.close()
            sys.exit(-1)

    f = open('temp.txt', 'w')
    print(script)
    script = '\n'.join(script)
    print(script)
    f.write(script)
    f.close()

    log.close()
    sys.exit(0)


except Exception as e:
    print("Error: " + str(e) + "\n")
    log.close()
    sys.exit(-1)
