pkill -9 run_datanode

nohup ./project/build/run_datanode 192.168.0.227 17550 > /dev/null 2>&1 &

