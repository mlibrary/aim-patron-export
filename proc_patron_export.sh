#!/bin/bash

echo "**** $0 started  " `date`
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [ $# = 1 ]
then
  DOTENV=$1
else
  DOTENV=$SCRIPT_DIR/.env
fi
START_TIME=`date '+%s'`

export $(grep -v '^#' $DOTENV | xargs -d '\n')
SCRIPT_NAME=`basename $0`
TODAY=`date`
OUT_BASE=patextract
FILE_PREFIX=EXPORT_USERS
echo "$SCRIPT_NAME : $TODAY : Export dir is $EXPORT_DIR, file prefix is $FILE_PREFIX"

# Alma writes the patron export in multiple files, with names of the
# form EXPORT_USERS-16422684150006381-1628579347274
# The first two segments (separated by a dash) are constant for a given export
# We'll count the number of sets in the directory by cutting the 1st 
# 2 segments from the file list, pipe to sort -u, and count them.  If the number 
# of "sets" is more than one, then write a message indicating multiple sets.  Then use the 
# 1st 2 segments of the latest set as the set name

cd $EXPORT_DIR

shopt -s nullglob
FILE_LIST=(${FILE_PREFIX}*)

if [[ -z $FILE_LIST ]] 
then
  echo $SCRIPT_NAME : no files to process
  exit
fi

SET_LIST=(`ls ${FILE_PREFIX}* | cut -d- -f1-2 | sort -u`)
NUM_OF_SETS=${#SET_LIST[@]}
echo last set is ${SET_LIST[-1]}
SET_NAME=${SET_LIST[-1]}
echo "number of sets in $EXPORT_DIR is $NUM_OF_SETS"
if [ $NUM_OF_SETS != 1 ]
then
  echo "WARNING: $NUM_OF_SETS patron exports in $EXPORT_DIR, using last: $SET_NAME"
fi
echo "SET_NAME is $SET_NAME"

cd $DATA_DIR
$PROG_DIR/proc_patron_export.pl -i $SET_NAME -d $EXPORT_DIR -o $SET_NAME > ${SET_NAME}.out 2>&1
cat ${SET_NAME}.report

cp ${SET_NAME}.illiad $ILLIAD_DIR
illiad_status=$?
cp ${SET_NAME}.libauth patextract.libauth
libauth_status=$?

echo "tar current set $SET_NAME to $EXPORT_DIR/processed"
cd $EXPORT_DIR
tar czf processed/${SET_NAME}.tar.gz ${SET_NAME}*
status=$?
if [ $status != 0 ]
then
  echo "ERROR: tar command returned status $status--files not removed"
  exit
fi

echo "Removing export files from $EXPORT_DIR"
rm ${SET_NAME}*
if [ $illiad_status -ne 0 ]
then
  exit
fi

if [ $libauth_status -ne 0 ]
then
  exit
fi

echo "**** $0 ended  " `date`
if [ "$SEND_METRICS" == "true" ]; then
  cat ${DATA_DIR}/${SET_NAME}.metrics | /usr/local/bin/pushgateway_advanced -j aim_patron_export
  /usr/local/bin/pushgateway -j patron_extract_processing -b $START_TIME 
fi
