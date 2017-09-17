#!/bin/bash

#####################################
#Input:
# 1) Folder where all part file save
# 2) Ouput filename
#How to run:
#bash dos2unix_process.sh  /home/mandar/Downloads/Spark_Example/resources/type2_result/part* tsp_result
#
##############################################


echo "Converting windows to linux format"

awk '{ sub("\r$", ""); print }' $1 > $2

echo "Finished processing"
