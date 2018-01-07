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
  echo "Done" >> $LOG
}
# install the SIGINT handler for ctrl+c
trap 'sigint_handler' 2

touch $LOG
i=0
while [ $RUNNING == "true" ]; do
  $CMD &
  PID=$!
  echo "Running $i, PID=$PID, CMD=$CMD" >> $LOG
  # ps
  wait $PID
  # echo "done running $i"
  ((i++))
done