#!/usr/bin/ksh
################################## CFF WRAPPER SCRIPT ####################################################
# Script Name	: cff_ingest_batch_main_load.ksh
# Description	: This script is used to run various Python Scripts based on the
#				: Load Types
# Input Params  : FILE_ID, TABLE_ID, LOAD_TYPE, EXEC_ENGINE,
#
#==============================================================================
# Modification History :
# Date			Description				Changed By
# ------------------------------------------------
# 04/21/2017	Initial Development     IBM
#
#################################################################################

. /anthem_dv/app/data_services/cff_pipeline/conf/ingest_batch.properties
. /anthem_dv/app/data_services/cff_pipeline/bin/ingest_utils.ksh $*


fn_loadParameters () {

	f_info_msg " Begin - Loading all parameters"

	for i in "$@"
	do
		case $i in
				--FILE_ID=*)
				export FILE_ID=${i#*=}
				shift
				;;
				--TABLE_ID=*)
				export TABLE_ID=${i#*=}
				shift
				;;
				--LOAD_TYPE=*)
				export LOAD_TYPE=${i#*=}
				shift
				;;
				--EXEC_ENGINE=*)
				export EXEC_ENGINE=${i#*=}
				shift
				;;
			*)
			f_abort "Error Loading the Parameters. Please check the Parameters"
			;;
		esac
	done

	f_info_msg " Loading NEW_BATCH values from INFO_FILE "
	INFO_FILE=${BATCH_DIR}/${FILE_ID}_info.txt
	source $INFO_FILE
	BATCH_ID=$NEW_BATCH

	f_info_msg " End - Loading all parameters"

}

# Start of Script
f_info_msg "*****************Start CFF ingest_batch_main script*********************"

#Check if all parameters are sent in request
if [[ $# -ne 4 ]] ; then
    f_abort "The script requires exactly 4 parameters to run, number of parameters sent in this request was $#"
fi

# Call function loadParameters to set the variables
fn_loadParameters $@


# Call replace, append, cdc Python Scripts based on Load Type
case "$LOAD_TYPE" in
	"replace")
	f_info_msg "Running the Python Script for LOAD_TYPE replace with parameters $FILE_ID $TABLE_ID $BATCH_ID $EXEC_ENGINE"
	fn_executePythonScript replace_2_table.py ${FILE_ID} $TABLE_ID $BATCH_ID $EXEC_ENGINE
	;;
	"append")
	f_info_msg "Running the Python Script for LOAD_TYPE append with parameters $FILE_ID $TABLE_ID $BATCH_ID $EXEC_ENGINE"
	fn_executePythonScript append_2_table.py ${FILE_ID} $TABLE_ID $BATCH_ID $EXEC_ENGINE
	;;
	"cdc")
	f_info_msg "Running the Python Script for LOAD_TYPE cdc with parameters $FILE_ID $TABLE_ID $BATCH_ID $EXEC_ENGINE"
	fn_executePythonScript cdc_2_table.py ${FILE_ID} $TABLE_ID $BATCH_ID $EXEC_ENGINE
	;;
	default)
	f_error_msg "Invalid LoadType $LOAD_TYPE sent"
	exit 1
	;;
esac


# End of main script
f_info_msg "*****************End CFF ingest_batch_main script*********************"
