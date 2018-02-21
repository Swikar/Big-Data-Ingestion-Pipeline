#!/usr/bin/ksh
################################## CFF WRAPPER SCRIPT ####################################################
# Script Name		: cff_ingest_batch_end.ksh
# Description		: This script is used to update LAST_BATCH_ID in the database,
#					:  and remove batch_ID file
# Input Parameter(s): FILE_ID
#
#=====================================================================================
# Modification History :
# Date			Description				Changed By
# ------------------------------------------------
#
#################################################################################

# TODO Get the input parameter
FILE_ID=$1

. /anthem_dv/app/data_services/cff_pipeline/conf/ingest_batch.properties
. /anthem_dv/app/data_services/cff_pipeline/bin/ingest_utils.ksh $*

f_info_msg "*****************Start CFF ingest_batch_end script*********************"

# Check if all parameters are sent in request
if [[ $# -ne 1 ]] ; then
    f_abort "The script requires exactly 1 parameters to run, number of parameters sent in this request was $#"
fi

f_info_msg "Getting BATCH_ID from info.txt "

INFO_FILE=${BATCH_DIR}/${FILE_ID}_info.txt
source $INFO_FILE
BATCH_ID=$NEW_BATCH
f_info_msg " End - Loading all parameters"


f_info_msg "Run Python Script - update_last_batch_id.py with parameters $FILE_ID $BATCH_ID "
fn_executePythonScript update_last_batch_id.py $FILE_ID $BATCH_ID

f_info_msg "Deleting the batch_ID file "
rm -f $INFO_FILE


f_info_msg "*****************End CFF ingest_batch_end script*********************"
