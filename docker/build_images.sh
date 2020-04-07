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
  docker build -t zjia/faas-worker-golang:$TAG -f ./docker/Dockerfile.worker-golang \
               --build-arg WATCHDOG_TAG=$TAG . )
( cd $ROOT_DIR &&
  docker build -t zjia/faas-worker-nodejs:$TAG -f ./docker/Dockerfile.worker-nodejs \
               --build-arg WATCHDOG_TAG=$TAG . )
( cd $ROOT_DIR &&
  docker build -t zjia/faas-golang-env:$TAG -f ./docker/Dockerfile.golang-env . )
( cd $ROOT_DIR &&
  docker build -t zjia/faas-nodejs-env:$TAG -f ./docker/Dockerfile.nodejs-env . )

docker tag zjia/faas-gateway:$TAG zjia/faas-gateway:latest
docker tag zjia/faas-watchdog-bionic:$TAG zjia/faas-watchdog-bionic:latest
docker tag zjia/faas-worker-golang:$TAG zjia/faas-worker-golang:latest
docker tag zjia/faas-worker-nodejs:$TAG zjia/faas-worker-nodejs:latest
docker tag zjia/faas-golang-env:$TAG zjia/faas-golang-env:latest
docker tag zjia/faas-nodejs-env:$TAG zjia/faas-nodejs-env:latest
