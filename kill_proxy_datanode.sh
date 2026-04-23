IN_MEMORY=0
IF_REDIS=0
SERVER='redis-server'
if [ $IF_REDIS -eq 0 ]; then
    SERVER='memcached'
fi

if [ $IN_MEMORY -eq 1 ]; then
  pkill -9 $SERVER
fi
pkill -9 run_datanode
pkill -9 run_proxy