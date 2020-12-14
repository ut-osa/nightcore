#!/bin/bash

BASE_DIR=$(realpath $(dirname $0))
NIGHTCORE_ROOT=$(realpath $(dirname $0)/../..)
BUILD_TYPE=release

rm -rf $BASE_DIR/outputs
mkdir -p $BASE_DIR/outputs

$NIGHTCORE_ROOT/bin/$BUILD_TYPE/gateway \
    --func_config_file=$BASE_DIR/func_config.json \
    --v=1 2>$BASE_DIR/outputs/gateway.log &

sleep 1

$NIGHTCORE_ROOT/bin/$BUILD_TYPE/engine \
    --func_config_file=$BASE_DIR/func_config.json \
    --node_id=0 \
    --v=1 2>$BASE_DIR/outputs/engine.log &

sleep 1

$NIGHTCORE_ROOT/bin/$BUILD_TYPE/launcher \
    --func_id=1 --fprocess_mode=cpp \
    --fprocess_output_dir=./outputs \
    --fprocess="$NIGHTCORE_ROOT/bin/$BUILD_TYPE/func_worker_v1 $BASE_DIR/libfoo.so" \
    --v=1 2>$BASE_DIR/outputs/launcher_foo.log &

$NIGHTCORE_ROOT/bin/$BUILD_TYPE/launcher \
    --func_id=2 --fprocess_mode=cpp \
    --fprocess_output_dir=./outputs \
    --fprocess="$NIGHTCORE_ROOT/bin/$BUILD_TYPE/func_worker_v1 $BASE_DIR/libbar.so" \
    --v=1 2>$BASE_DIR/outputs/launcher_bar.log &

wait
