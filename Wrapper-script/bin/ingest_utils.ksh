#!/usr/bin/ksh
################################## CFF WRAPPER SCRIPT ####################################################
# Script Name	: ingest_utils.ksh
# Description	: Common functions used by Ingestion process driver programs
#==============================================================================
# Modification History :
# Date			Description				Changed By
# ------------------------------------------------
# 04/21/2017	Initial Development     IBM
#
#################################################################################

# ------------------------------------------------------------------------------
# message logging functions. First argument is the message to display on stderr.
# f_abort takes an optional second argument that is the error-code with which
# the script exits. By default script will exit with error cdoe 1
# ------------------------------------------------------------------------------
_echo_msg() {
	
  CFF_MSG="$1: $(date "+%F %T"): $2"
  echo $CFF_MSG >> ${LOG_FILE}
  echo $CFF_MSG
}
f_info_msg() {
	_echo_msg "INFO" "$1"
}
f_warn_msg() {
	_echo_msg "WARN" "$1"
}
f_error_msg() {
	_echo_msg "ERROR" "$1"
}
f_abort() {
	env 1>&2
	f_error_msg "$1"
	exit ${2:-1}
}

fn_executePythonScript () {
	f_info_msg "--------------------------------------------------------------"
	f_info_msg "Begin - Executing Python Script - $1"

	cd $PYTHON_BIN
	python $*  >> ${LOG_FILE}
	if [ ! "$?" -eq "0" ]; then
		f_abort "Data Ingestion failed while executing the Python Script $1."
	fi
	cd -

	f_info_msg "Success : End - Executing Python Script - $1"
	f_info_msg "--------------------------------------------------------------"
}
