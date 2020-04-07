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

export CFLAGS="-fdata-sections -ffunction-sections"
export CXXFLAGS="-fdata-sections -ffunction-sections"

rm -rf ${DEPS_INSTALL_PATH}
mkdir -p ${DEPS_INSTALL_PATH}

# Build abseil-cpp
cd $BASE_DIR/deps/abseil-cpp && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/abseil-cpp/build

# Build http-parser
cd $BASE_DIR/deps/http-parser && make clean && make package && \
  install -D $BASE_DIR/deps/http-parser/http_parser.h $DEPS_INSTALL_PATH/include/http_parser.h && \
  install -D $BASE_DIR/deps/http-parser/libhttp_parser.a $DEPS_INSTALL_PATH/lib/libhttp_parser.a && \
  make clean

# Build libuv
cd $BASE_DIR/deps/libuv && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DLIBUV_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/libuv/build

# Build nghttp2
cd $BASE_DIR/deps/nghttp2 && rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DENABLE_LIB_ONLY=ON \
        -DENABLE_ASIO_LIB=OFF -DENABLE_STATIC_LIB=ON -DWITH_JEMALLOC=OFF \
        -DCMAKE_INSTALL_PREFIX=${DEPS_INSTALL_PATH} -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf $BASE_DIR/deps/nghttp2/build
