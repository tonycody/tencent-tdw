#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

for f in $bin/../lib/*.*; do
   CLASSPATH=${CLASSPATH}:$f;
done;
nohup java -classpath "$CLASSPATH" IndexService.IndexServer $@ &


