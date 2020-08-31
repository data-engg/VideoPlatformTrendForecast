#!/bin/sh
if [ $# -ne 1 ]
then
        echo "Usage : $0 <project path>"
        echo "Example : ./data_gen_execute.py ./"
        exit 1
fi
DIR=$1
PYSCRIPT=${DIR}/data_generator/data_gen_execute.py
LOGPATH=${DIR}/data_generator/log/data_gen.log
OUTPATH=${DIR}/data_generator/out
SPOOLPATH=${DIR}/data_generator/spoolout
if [ ! -f $LOGPATH ]
then
        touch $LOGPATH
fi
echo "$(date '+%d:%m:%Y %H:%M:%S') project path : ${DIR}" >> $LOGPATH
echo "$(date '+%d:%m:%Y %H:%M:%S') pyscript path : ${DIR}/data_generator/data_gen_execute.py" >> $LOGPATH
if [ ! -f $PYSCRIPT ]
then
        echo "$PYSCRIPT : not found... Exiting script" >> $LOGPATH
    exit 1
fi
while true
do
        python $PYSCRIPT >> $LOGPATH
        if [ $? == 0 ]
        then
                mv $OUTPATH/* $SPOOLPATH/
        else
                echo "Exiting script... Error in python script" >> $LOGPATH
                exit 1
        fi
        sleep 5
done
