#allison:this is the standard start Hive Server script
#default port is 50000
export LANG=zh_CN.utf8
export LC_ALL=zh_CN.utf8
port=${1:-50000}
server_home=$(cd "$(dirname $0)/../" && pwd)

cd "$server_home" && nohup bin/hive --service hiveserver $port &

