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

def filter_col_desc(col_desc):
    p_list = ['\n',u'\xa0',u'\xbf',"'"]
    for x in p_list:
        col_desc = col_desc.replace(x,'')
    return col_desc


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
    SQL_STR = "SELECT table_id, table_name, hive_db FROM target_table"
    SQL_STR += " WHERE table_id=" + P_FILE_ID
    cursor.execute(SQL_STR)
    for (table_id,table_name,hive_db) in cursor:
        t_id = table_id
        t_n = table_name
        hdb = hive_db
    # Start the DDL with CREATE EXTERNAL TABLE
    print("Generating DDL... \n")
    ADMIN_STR = 'set role admin;\n'
    DDL_STR = "CREATE TABLE " + hdb + "." + t_n + " ( "

    # Generate a MySQL SELECT to obtain all external table column metadata
    SQL_STR = "SELECT col_name, d_type,col_desc FROM target_table_def "
    SQL_STR += " WHERE table_id=" + P_FILE_ID
    SQL_STR += " ORDER BY col_order"
    cursor.execute(SQL_STR)
    # Add columns and corresponding data type onto DDL
    for (col_name, d_type,col_desc) in cursor:
        DDL_STR += col_name + " " + d_type + " COMMENT '" + filter_col_desc(col_desc) + "', " 

    cnx.close()
    print("MySQL Connection Closed \n")

    # Finish DDL
    DDL_STR = DDL_STR[:-2]
    DDL_STR += ")\n"
    DDL_STR += 'partitioned by (batch_id bigint)\n'
    DDL_STR += "stored as orc\n"
    DDL_STR += 'tblproperties ("orc.compress"="ZLIB",\n'
    DDL_STR += '"orc.stripe.size"="67108864",\n'
    DDL_STR += '"orc.row.index.stride"="10000",\n'
    DDL_STR += '"orc.create.index"="true")\n'
    GRANT_STR = 'grant all on ' + hdb + '.' + t_n + ' to role bddveng1\n'
    print(GRANT_STR)
    print("DDL Creation Successful \n")

    # Generate a DROP string to drop the table if it exists
    DROP_STR = "DROP TABLE IF EXISTS " + hdb + "." + t_n

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

    cursor.execute(GRANT_STR)
    cnx.close()
    print("Hive Connection Closed \n")
    print("Target Table " + P_FILE_ID + " Created \n")
    log.close()
    sys.exit(0)


except Exception as e:
    print("Error: " + str(e) + "\n")
    log.close()
    sys.exit(-1)
