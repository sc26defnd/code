CRT_DIR=$(pwd)
set -e

IF_JERASURE=1
IF_YALANTING=1
IF_MEMCACHED=0
IF_REDIS=0

SOCKET_RPC_INSTALL_DIR=$CRT_DIR"/project/third_party/yalantinglibs"
GF_INSTALL_DIR=$CRT_DIR"/project/third_party/gf-complete"
JERASURE_INSTALL_DIR=$CRT_DIR"/project/third_party/jerasure"
HIREDIS_INSTALL_DIR=$CRT_DIR"/project/third_party/hiredis"
REDIS_PLUS_PLUS_INSTALL_DIR=$CRT_DIR"/project/third_party/redis-plus-plus"
REDIS_INSTALL_DIR=$CRT_DIR"/project/third_party/redis"
LIBMEMCACHED_INSTALL_DIR=$CRT_DIR"/project/third_party/libmemcached"
MEMCACHED_INSTALL_DIR=$CRT_DIR"/project/third_party/memcached"

SOCKET_RPC_DIR=$CRT_DIR"/third_party/yalantinglibs"
GF_DIR=$CRT_DIR"/third_party/gf-complete"
JERASURE_DIR=$CRT_DIR"/third_party/Jerasure"
HIREDIS_DIR=$CRT_DIR"/third_party/hiredis"
REDIS_PLUS_PLUS_DIR=$CRT_DIR"/third_party/redis-plus-plus"
REDIS_DIR=$CRT_DIR"/third_party/redis"
LIBMEMCACHED_DIR=$CRT_DIR"/third_party/libmemcached-1.0.18"
MEMCACHED_DIR=$CRT_DIR"/third_party/memcached-1.6.31"

mkdir -p $CRT_DIR"/third_party"

if [ $IF_JERASURE -eq 1 ]; then
  # gf-complete
  mkdir -p $GF_INSTALL_DIR
  cd $GF_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf gf-complete
  git clone https://github.com/ceph/gf-complete.git
  cd $GF_DIR
  autoreconf -if
  ./configure --prefix=$GF_INSTALL_DIR
  make -j6
  make install

  # jerasure
  mkdir -p $JERASURE_INSTALL_DIR
  cd $JERASURE_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf Jerasure
  git clone https://github.com/tsuraan/Jerasure.git
  cd $JERASURE_DIR
  autoreconf -if
  ./configure --prefix=$JERASURE_INSTALL_DIR LDFLAGS=-L$GF_INSTALL_DIR/lib CPPFLAGS=-I$GF_INSTALL_DIR/include
  make -j6
  make install
fi

if [ $IF_YALANTING -eq 1 ]; then
  # yalantinglibs, for socket & rpc
  sudo apt-get install libprotobuf-dev protobuf-compiler libprotoc-dev
  mkdir -p $SOCKET_RPC_INSTALL_DIR
  cd $SOCKET_RPC_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf yalantinglibs
  git clone https://github.com/alibaba/yalantinglibs.git
  cd $SOCKET_RPC_DIR
  mkdir build && cd build
  cmake .. -DCMAKE_INSTALL_PREFIX=$SOCKET_RPC_INSTALL_DIR
  cmake --install .
fi

if [ $IF_REDIS -eq 1 ]; then
  # hiredis
  mkdir -p $HIREDIS_INSTALL_DIR
  cd $HIREDIS_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf hiredis
  git clone https://github.com/redis/hiredis.git
  cd $HIREDIS_DIR
  make PREFIX=$HIREDIS_INSTALL_DIR
  make PREFIX=$HIREDIS_INSTALL_DIR install

  # redis-plus-plus
  mkdir -p $REDIS_PLUS_PLUS_INSTALL_DIR
  cd $REDIS_PLUS_PLUS_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf redis-plus-plus
  git clone https://github.com/sewenew/redis-plus-plus.git
  cd $REDIS_PLUS_PLUS_DIR
  mkdir build && cd build
  cmake -DCMAKE_PREFIX_PATH=$HIREDIS_INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$REDIS_PLUS_PLUS_INSTALL_DIR ..
  cmake --build . -j8
  cmake --install .

  # redis
  mkdir -p $REDIS_DIR
  cd $REDIS_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf redis
  git clone https://github.com/redis/redis.git
  cd $REDIS_DIR
  make PREFIX=$REDIS_INSTALL_DIR install
fi

if [ $IF_MEMCACHED -eq 1 ]; then
  # libmemcached
  mkdir -p $LIBMEMCACHED_INSTALL_DIR
  cd $LIBMEMCACHED_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf libmemcached-1.0.18
  found=$(find . -name "libmemcached-1.0.18.tar.gz")
  if [ -n "$found" ]; then
    echo "libmemcached-1.0.18.tar.gz is already downloaded"
  else
    wget https://launchpad.net/libmemcached/1.0/1.0.18/+download/libmemcached-1.0.18.tar.gz
  fi
  tar -zxvf libmemcached-1.0.18.tar.gz
  cd $LIBMEMCACHED_DIR
  ./configure --prefix=$LIBMEMCACHED_INSTALL_DIR
  make -j6
  make install

  # memcached
  sudo apt-get install libevent-dev
  mkdir -p $MEMCACHED_INSTALL_DIR
  cd $MEMCACHED_INSTALL_DIR
  rm * -rf
  cd $CRT_DIR"/third_party"
  rm -rf memcached-1.6.31
  found=$(find . -name "memcached-1.6.31.tar.gz")
  if [ -n "$found" ]; then
    echo "memcached-1.6.31.tar.gz is already downloaded"
  else
    wget http://memcached.org/files/memcached-1.6.31.tar.gz
  fi
  tar -zxvf memcached-1.6.31.tar.gz
  cd $MEMCACHED_DIR
  ./configure --prefix=$MEMCACHED_INSTALL_DIR
  make -j6
  make install
fi

# clean
rm -rf $CRT_DIR"/third_party"