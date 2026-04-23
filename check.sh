#!/bin/bash
set -e
# nodes for testbed
ARRAY=('192.168.0.219' '192.168.0.220' '192.168.0.221' '192.168.0.222' '192.168.0.223' '192.168.0.224' '192.168.0.225' '192.168.0.226' '192.168.0.227')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`

for i in $(seq 0 $NUM)
do
temp=${ARRAY[$i]}
    echo $temp
    ssh roe@$temp 'ps -aux | grep run_datanode | wc -l'
    ssh roe@$temp 'ps -aux | grep run_proxy | wc -l'
done