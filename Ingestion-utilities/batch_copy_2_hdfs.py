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
    SQL_STR = "SELECT file_id, file_name, ext_table_name, delim, input_path, hive_db FROM ext_table t1"
    #THIS LINE FOR TESTING ONLY:
    # SQL_STR += ' WHERE file_id LIKE "8%"'
    if(P_FILE_ID != None):
        SQL_STR += " WHERE file_id=" + P_FILE_ID
    cursor.execute(SQL_STR)
    for x in cursor:
        files.append(x)
    # print(files)
    # subprocess.call(["python", "hello.py"])
    for (file_id, file_name, ext_table_name, delim, input_path, hive_db) in files:
        f_id = file_id
        fn = file_name
        etn = ext_table_name
        d = delim
        ip = input_path
        hdb = hive_db

        hdbetn = hdb + "." + etn

        try:
            #GET SUMS FROM RAW DATA FILE
            #command = ["hdfs", "dfs", "-mkdir", "-p", ip]
            #subprocess.call(command)
            file_n=""
            files = os.listdir(ip)
            if (len(files)>0):
              if (len(files)>1):
                for x in files:
                  if (".txt" in x):
                    file_n = x
              else :
                file_n = files[0]
              #print(file_n)
              floc = ip + file_n
              command = ["python", "copy_2_hdfs.py", floc, ip]
              print(' '.join(command))
              subprocess.call(command)

            
            #command = ["python", "create_ext_table.py", str(f_id)]
            #print(' '.join(command))
            #subprocess.call(command)
            

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
