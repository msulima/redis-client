#!/bin/sh

#REDIS_HOST="localhost"
REDIS_HOST="ec2-52-211-87-152.eu-west-1.compute.amazonaws.com"
REDIS_PATH=~/bin/redis-3.0.6
PORTS=(30001 30002 30003)
#PORTS=(6379)

function sum_stat {
  for i in ${PORTS[@]}; do
    $REDIS_PATH/src/redis-cli -h $REDIS_HOST -p $i info | grep $1 | cut -d ':' -f 2;
  done | tr '\r' ' ' | paste -sd+ - | bc
}

PERIOD=$1

while true; do
  COUNTER=0
  START_TIME=$(date +%s)
  START=$(sum_stat total_commands_processed)

  while [ "$COUNTER" -lt "$PERIOD" ]; do
    sum_stat instantaneous_ops_per_sec
    let COUNTER=COUNTER+1
  done

  STOP=$(sum_stat total_commands_processed)
  STOP_TIME=$(date +%s)
  TOTAL=$(echo "($STOP-$START-${#PORTS[@]}*($PERIOD+1))" | bc)
  echo total $TOTAL in $(echo "$STOP_TIME-$START_TIME" | bc) seconds
  echo average $(echo "$TOTAL/($STOP_TIME-$START_TIME)" | bc)
done
