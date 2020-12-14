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

PYTHONPATH=$NIGHTCORE_ROOT/worker/python $NIGHTCORE_ROOT/bin/$BUILD_TYPE/launcher \
    --func_id=1 --fprocess_mode=go \
    --fprocess_output_dir=$BASE_DIR/outputs \
    --fprocess="python3 $BASE_DIR/main.py" \
    --v=1 2>$BASE_DIR/outputs/launcher_foo.log &

PYTHONPATH=$NIGHTCORE_ROOT/worker/python $NIGHTCORE_ROOT/bin/$BUILD_TYPE/launcher \
    --func_id=2 --fprocess_mode=go \
    --fprocess_output_dir=$BASE_DIR/outputs \
    --fprocess="python3 $BASE_DIR/main.py" \
    --v=1 2>$BASE_DIR/outputs/launcher_bar.log &

wait
