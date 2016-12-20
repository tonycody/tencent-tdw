#!/bin/bash

if fuser $0 2>/dev/null |sed "s/\\<$$\\>//" |grep -q '[0-9]';then
    echo "Script is already running..." 1>&2
    exit 1
fi

if [ $# -ne 1 ]
then
    echo "USAG:$0 port"
    exit
fi

port=$1

shellLocation=`dirname $0`
shellLocation=`cd "$shellLocation" ; pwd`

cd $shellLocation && ./stop-server.sh $port

proccount=`/usr/bin/lsof -i:$port|grep LISTEN | grep -v grep  -c`

if [ $proccount -eq 0 ]
then
    echo "Now , starting $port ...."
    cd $shellLocation && ./start-server.sh $port &
    sleep 2
    proccount=`/usr/bin/lsof -i:$port|grep LISTEN | grep -v grep  -c`
    if [ $proccount -eq 1 ]
    then
        echo "Start $port success!"
    fi
fi
