pkill -9 run_proxy

nohup ./project/build/run_proxy 192.168.0.223 32406 192.168.0.227:17550 > /dev/null 2>&1 &

