#!/bin/bash

if [ $# -ne 1 ]
then
    echo "USAG:$0 port"
    exit
fi

port=$1

echo "Now , stoping $port ...."

/usr/bin/lsof -i:$port|grep LISTEN | grep -v grep | awk '{print "kill -9 "$2}'|sh

sleep 1

proccount=`/usr/bin/lsof -i:$port|grep LISTEN | grep -v grep  -c`

if [ $proccount -eq 0 ]
then
    echo "Stop success!"
else
    echo "Stop failed!"
    exit
fi
