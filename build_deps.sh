#!/bin/bash

SCRIPT_PATH=$(readlink -f $0)
BASE_DIR=$(dirname $SCRIPT_PATH)
CMAKE_BUILD_TYPE="Release"
DEPS_INSTALL_PATH=$BASE_DIR/deps/out

while [ ! $# -eq 0 ]
do
	case "$1" in
		--debug)
      CMAKE_BUILD_TYPE="Debug"
			;;
	esac
	shift
done

rm -rf ${DEPS_INSTALL_PATH}
mkdir -p ${DEPS_INSTALL_PATH}

# Build json
cd $BASE_DIR/deps/json && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DJSON_BuildTests=OFF -DCMAKE_CXX_STANDARD=11 \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/json/build

# Build abseil-cpp
cd $BASE_DIR/deps/abseil-cpp && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_STANDARD=11 \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/abseil-cpp/build

# Build http-parser
cd $BASE_DIR/deps/http-parser && make clean && make package && \
  install -D $BASE_DIR/deps/http-parser/http_parser.h $DEPS_INSTALL_PATH/include/http_parser.h && \
  install -D $BASE_DIR/deps/http-parser/libhttp_parser.a $DEPS_INSTALL_PATH/lib/libhttp_parser.a && \
  make clean

# Build glog
cd $BASE_DIR/deps/glog && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DWITH_GFLAGS=OFF -DCMAKE_CXX_STANDARD=11 \
        -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/glog/build

# Build libuv
cd $BASE_DIR/deps/libuv && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DLIBUV_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/libuv/build
