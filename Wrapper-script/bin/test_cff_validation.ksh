#!/usr/bin/ksh
#Script name:
#Description:
#
#Input params: FILE_ID,BATCH_ID,TABLE_ID
#=========================================
#Modification History :
# Date								Description 													Changed By
#--------------------------------------
# 08/03/2017 					Initial Development  IBM  
#################################################################################
FILE_ID=$1
BATCH_ID=$2
TABLE_ID=$3

# Check if all params are loaded
if [[ $# -ne 3 ]] ; then
    echo "The script requires at least 3 parameter to run, number of parameters entered is $#"
    exit 1
fi
########Querying MySql for database and table name#############

echo "Querying MysSql..."

TRG_TEMP=`exec mysql -N -s --host=sl01dlvbim004.wellpoint.com  --user=AF43199  --database=anthem_dv_data_services -e "select hive_db, table_name from target_table  where table_id=${FILE_ID}0;"`

set -A TRG_VALUES $TRG_TEMP
TRG_DB=${TRG_VALUES[0]}
TRG_TABLE_NAME=${TRG_VALUES[1]}
echo "$TRG_TEMP"

echo "---------------------------------------------------------
echo "Display first 10 records from each table..."

TRG_TEN=`exec -S -e "select * from anthem_dv_raw_wh.$TRG_TABLE_NAME limit 10;"`
VW_TEN=`exec -S -e "select * from anthem_dv_curated_temp.$VW_TABLE_NAME limit 10;"`
#CFF_TEN=`exec -S -e "select * from anthem_curated_wh.$CFF_TABLE_NAME limit 10;"`

echo -e "\nFirst 10 rows from Target table"
echo "$TRG_TEN"
echo -e "\nFirst 10 rows from View table"
echo "$VW_TEN"
echo -e "\nFirst 10 rows from CFF table"
#echo "$CFF_TEN"

echo "------------------------------------------------------"
echo -e "\nGathering Record count for target, view and CFF tables..."

typeset -i TRG_TABLE_CNT=`exec hive -S -e "select count(*) from anthem_dv_raw_wh.$TRG_TABLE_NAME;"`
 
typeset -i VW_TABLE_CNT=`exec hive -e "select count(*) from anthem_dv_curated_temp.$VW_TABLE_NAME;"`

typeset -i CFF_TABLE_CNT=`exec hive -e "select c.batch_id, count(*) from anthem_dv_curated_wh.$CFF_TABLE_NAME as c join (select max(clm.batch_id) as batch_id from anthem_dv_curated_wh.$CFF_TABLE_NAME clm) c2 on c2.batch_id= c.batch_id where table_id=${TABLE_ID}99 group by c.batch_id;"`

if [[$TRG_TABLE_CNT -eq $VW_TABLE_CNT]]; then
    echo "Successful: number of target table records and view table records match"
else 
    echo "Failure: number of target table records and view table records do not match"
fi

if [[$VW_TABLE_CNT -eq $CFF_TABLE_CNT ]]; then
    echo "Successful: number of view records and CFF table records match"
else
    echo "Failure: number of view table records and CFF table records do not match"
fi


### Print Records from View and target table ###############

echo "#################################################"

echo -e "\Displaying Record count for Target table"
echo "$TRG_TABLE_CNT"

echo -e "\nDiplaying Record count for View table"
echo "$VW_TABLE_CNT"

echo -e "\nDisplaying Record Count for CFF table"
echo "$CFF_TABLE_CNT"
echo "#################################################"
