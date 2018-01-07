#!/bin/bash

CMD="node ../js/gdax_subscription_app.js"
PID="null"
RUNNING="true"
LOG='run_process_log'


function sigint_handler() {
  if [ "$PID" != "null" ]; then
    echo "kill -15 $PID"
    kill -15 $PID
  fi
  RUNNING="false"
  ts=$(date +"%Y%m%d-%T")
  echo "[$ts] Done" >> $LOG
}
# install the SIGINT handler for ctrl+c
trap 'sigint_handler' 2

rm -f $LOG
touch $LOG

i=0
while [ $RUNNING == "true" ]; do
  $CMD &
  PID=$!
  ts=$(date +"%Y%m%d-%T")
  echo "[$ts] Running $i, PID=$PID, CMD=$CMD" >> $LOG
  # ps
  wait $PID
  # echo "done running $i"
  ((i++))
done