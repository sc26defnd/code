# run client
./project/build/run_client config.ini 100 6
# kill coordinator
pkill -9 run_coordinator
# unlimit bandwidth
bash exp.sh 6
# kill datanodes and proxies
bash exp.sh 0

