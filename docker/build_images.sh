#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`
GIT_HASH=`git rev-parse --short HEAD`
DATE=`date +"%Y%m%d"`
TAG=git-$GIT_HASH-$DATE

( cd $ROOT_DIR &&
  docker build -t zjia/faas-gateway:$TAG -f ./docker/Dockerfile.gateway . )
( cd $ROOT_DIR &&
  docker build -t zjia/faas-watchdog-bionic:$TAG -f ./docker/Dockerfile.watchdog-bionic . )
( cd $ROOT_DIR &&
  docker build -t zjia/faas-watchdog-alpine:$TAG -f ./docker/Dockerfile.watchdog-alpine . )
