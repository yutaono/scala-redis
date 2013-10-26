#!/usr/bin/env bash

set -e
trap 'kill $PID0 $PID1 $PID2 $PID3 $PID4 $PID5 $PID6 $PID7 > /dev/null 2>&1' EXIT

function start_node {
    redis-server --port $1 --save "" > /dev/null 2>&1 &
}

function start_sentinel {
    redis-server --sentinel --port $1 --sentinel monitor scala-redis-test localhost 6379 2 > /dev/null 2>&1 &
}

start_node 6379
PID0=$!
start_node 6380
PID1=$!
start_node 6381
PID2=$!
start_node 6382
PID3=$!
start_sentinel 26379
PID4=$!
start_sentinel 26380
PID5=$!
start_sentinel 26381
PID6=$!
start_sentinel 26382
PID7=$!

sbt test
