#!/usr/bin/ksh
################################## CFF WRAPPER SCRIPT ####################################################
# Script Name		: cff_ingest_batch_start.ksh
# Description		:  call python script to get
#					: BATCH_ID and then return the CURRENT_BATCH_ID
# Input Parameter(s): FILE_ID
#
#=====================================================================================
# Modification History :
# Date			Description				Changed By
# ------------------------------------------------
# 04/17/2017	Initial Development		IBM
#
#################################################################################

# Get the input parameter
FILE_ID=$1



. /anthem_dv/app/data_services/cff_pipeline/conf/ingest_batch.properties
. /anthem_dv/app/data_services/cff_pipeline/bin/ingest_utils.ksh $*

f_info_msg "*****************Start CFF ingest_batch_start script*********************"
# Check if all parameters are sent in request
if [[ $# -ne 1 ]] ; then
    f_abort "The script requires exactly 1 parameters to run, number of parameters sent in this request was $#"
fi


# Run python script to update and get the current Batch_ID
f_info_msg "==========Run Python script - get_current_batch_id.py with parameter $FILE_ID=========="

# Reading python files
cd $PYTHON_BIN
temp=`exec python get_current_batch_id.py $FILE_ID`
if [ ! "$?" -eq "0" ]; then
	f_abort "Data Ingestion failed while executing the Python Script $1. Please refer to the Python Script log for more details. $temp"
fi
CURRENT_BATCH_ID=`exec echo $temp | tail -c 9`
cd -

f_info_msg "Python script output - $temp"
f_info_msg "current_batch_id is ${CURRENT_BATCH_ID}"
f_info_msg "========Success : End - Executing Python Script get_current_batch_id.py with parameter $FILE_ID========"
f_info_msg "Writing batch_ID to info.txt"
echo "set -a" > $INFO_FILE
echo "NEW_BATCH=$CURRENT_BATCH_ID" >> $INFO_FILE

if [ ! "$?" -eq "0" ]; then
	f_abort "Error writing current batch ID to batch ID file"
fi

f_info_msg "*****************End CFF ingest_batch_start script*********************"
