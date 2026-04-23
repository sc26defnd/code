ARRAY=('192.168.0.219' '192.168.0.220' '192.168.0.221' '192.168.0.222' '192.168.0.223' '192.168.0.224' '192.168.0.225' '192.168.0.226' '192.168.0.227')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`
SRC_PATH1=/home/defend/ec_prototype/run_cluster_sh/
SRC_PATH2=/home/defend/ec_prototype/project
SRC_PATH3=/home/defend/wondershaper/

DIS_DIR1=/home/defend/ec_prototype
DIS_DIR2=/home/defend/wondershaper

IF_SERVER=0
IF_REDIS=0
SERVER='redis-server'
if [ $IF_REDIS -eq 0 ]; then
    SERVER='memcached'
fi

# if simulate cross-cluster transfer
if [ $1 == 1 ]; then
    echo "cluster_number:"${#ARRAY[@]}
    for i in $(seq 0 $NUM)
    do
        temp=${ARRAY[$i]}
        echo $temp
        ssh root@$temp 'cd /home/defend/ec_prototype;bash cluster_run_datanode.sh;'
        echo 'server&datanode process number:'
        ssh root@$temp 'ps -aux | grep' ${SERVER} '| wc -l;ps -aux | grep run_datanode | wc -l;'
    done
    for i in $(seq 0 $NUM)
    do
        temp=${ARRAY[$i]}
        echo $temp
        if [ $temp != '192.168.0.227' ]; then
          ssh root@$temp 'cd /home/defend/ec_prototype;bash cluster_run_proxy.sh;'
          echo 'proxy process number:'
          ssh root@$temp 'ps -aux | grep run_proxy | wc -l'
        fi
    done
elif [ $1 == 5 ]; then  # for networkcore
    ssh root@192.168.0.227 '/home/defend/wondershaper/wondershaper/wondershaper -c -a eth0; /home/defend/wondershaper/wondershaper/wondershaper -a eth0 -d 2500000 -u 2500000'
elif [ $1 == 6 ]; then
    ssh root@192.168.0.227 '/home/defend/wondershaper/wondershaper/wondershaper -c -a eth0;echo done'
else
    echo "cluster_number:"${#ARRAY[@]}
    for i in $(seq 0 $NUM)
    do
    temp=${ARRAY[$i]}
        echo $temp
        if [ $1 == 0 ]; then
            if [ $IF_SERVER == 1 ]; then
              if [ $temp == '192.168.0.227' ]; then
                  ssh root@$temp 'pkill -9 run_datanode;pkill -9' ${SERVER}
              else
                  ssh root@$temp 'pkill -9 run_datanode;pkill -9 run_proxy;pkill -9' ${SERVER}
              fi
            else
              if [ $temp == '192.168.0.227' ]; then
                  ssh root@$temp 'pkill -9 run_datanode;'
              else
                  ssh root@$temp 'pkill -9 run_datanode;pkill -9 run_proxy'
              fi
            fi
            echo 'pkill  all'
            ssh root@$temp 'ps -aux | grep' ${SERVER} '| wc -l'
            ssh root@$temp 'ps -aux | grep run_datanode | wc -l'
            ssh root@$temp 'ps -aux | grep run_proxy | wc -l'
        elif [ $1 == 2 ]; then
            ssh root@$temp 'mkdir -p' ${DIS_DIR1}
            ssh root@$temp 'mkdir -p' ${DIS_DIR2}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_datanode.sh root@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy.sh root@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH2} root@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH3} root@$temp:${DIS_DIR2}
        elif [ $1 == 3 ]; then   # if not simulate cross-cluster transfer
            ssh root@$temp '/home/defend/wondershaper/wondershaper/wondershaper -c -a eth0; /home/defend/wondershaper/wondershaper/wondershaper -a eth0 -d 2500000 -u 2500000'
        elif [ $1 == 4 ]; then
            ssh root@$temp '/home/defend/wondershaper/wondershaper/wondershaper -c -a eth0;echo done'
        elif [ $1 == 7 ]; then
            ssh root@$temp 'cd /home/defend/ec_prototype/storage/;rm -r *'
        fi
    done
fi