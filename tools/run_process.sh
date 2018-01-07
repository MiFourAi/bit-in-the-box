#!/bin/bash

CMD="node ../js/gdax_subscription_app.js"
PID="null"
RUNNING="true"

function sigint_handler() {
  if [ "$PID" != "null" ]; then
    echo "kill -15 $PID"
    kill -15 $PID
  fi
  RUNNING="false"
}
trap 'sigint_handler' 2

i=0
while [ $RUNNING == "true" ]; do
  $CMD &
  PID=$!
  echo "Running $i, PID=$PID, CMD=$CMD"
  # ps
  wait $PID
  # echo "done running $i"
  ((i++))
done