CRT_DIR=$(pwd)
RESULT_PATH=$CRT_DIR"/res/"
if [ ! -d $RESULT_PATH ]; then
  mkdir -p $RESULT_PATH
fi

# limit bandwidth
bash exp.sh 5
# run datanodes and proxies
bash exp.sh 1
# run coordinator
./project/build/run_coordinator >> $RESULT_PATH"coordinator.log" 2>&1 &