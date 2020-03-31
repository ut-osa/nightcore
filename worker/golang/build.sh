#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)`

mkdir -p $ROOT_DIR/build
( cd $ROOT_DIR &&
  go build -a -ldflags "-s -w" -o $ROOT_DIR/build/main )
