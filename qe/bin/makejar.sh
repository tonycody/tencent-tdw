#/bin/sh

BIN=$(dirname "$0")
HIVE_HOME=$(dirname $(cd $BIN;pwd))

export PATH=$JAVA_HOME/bin:/usr/local/bin:$PATH:$HIVE_HOME/bin

if [ $# -ne 8 ]
then
    echo "Usage: sh $0 Url User Passwd DBName TableName UserName protofile protoversion"
    echo "for example:
		      makejar.sh jdbc:postgresql://localhost:5432/pbjar tdwmeta tdwmeta default_db person root person.proto 2.5.0"
    exit 1
fi

Url=$1
User=$2
Passwd=$3
DBName=$4
TableName=$5
UserName=$6
FileName=$7
ProtoVersion=$8

protoc_version="$(protoc --version | sed 's/libprotoc //g')"

if [[ "$protoc_version" = "$ProtoVersion" ]];then
	true
else
	echo "error! expect protobuf version $ProtoVersion,but get $protoc_version!"
	exit 7
fi

cd $HIVE_HOME 

if [ ! -d ./protobuf/gen/$DBName ]
then
    mkdir -p ./protobuf/gen/${DBName}
fi

ModifiedTime=$(date +%Y%m%d%H%M%S)

protoc --plugin=protoc-gen-tdw=./bin/protoc-gen-tdw --tdw_out=database=${DBName},table=${TableName},time=${ModifiedTime}:./protobuf/gen/${DBName} --proto_path=./protobuf/upload/${UserName}/ ./protobuf/upload/${UserName}/${FileName}
if [ $? -ne 0 ]
then
    rm -rf $HIVE_HOME/protobuf/upload/${UserName}/*.proto 
    exit 2
fi

NewFileName=$TableName".proto"

cd $HIVE_HOME/protobuf
if [ -d tdw ]
then
    rm -rf tdw/*
fi

cd $HIVE_HOME
if [ ! -f ./protobuf/gen/${DBName}/${NewFileName} ]
then
    rm -rf $HIVE_HOME/protobuf/upload/${UserName}/*.proto
    echo "$NewFileName not exist"
    exit 3
fi 

protoc --java_out=./protobuf/ --proto_path=./protobuf/gen/${DBName} ./protobuf/gen/${DBName}/${NewFileName} 

if [ $? -ne 0 ]
then 
    rm -rf $HIVE_HOME/protobuf/upload/${UserName}/*.proto
    echo "protoc cmd2 error, Please contact to administrator."
    exit 4
fi

javac -classpath ./lib/protobuf-java-${ProtoVersion}.jar ./protobuf/tdw/*.java
if [ $? -ne 0 ]
then 
    rm -rf $HIVE_HOME/protobuf/upload/${UserName}/*.proto
    echo "javac cmd error, Please contact to administrator."
    exit 5
fi

if [ ! -d ./auxlib ]
then
    mkdir ./auxlib
fi

JarName=${DBName}_${TableName}_${ModifiedTime}.jar
cd ./protobuf
jar cf ../auxlib/$JarName ./tdw/*.class
cd ..
cwp=`pwd`
echo ${cwp}/auxlib/$JarName

cd $HIVE_HOME
java -classpath $(ls ./lib/postgresql-*.jar):./lib/hive_contrib.jar org.apache.hadoop.hive.contrib.uploadjar.uploadjar ${Url} ${User} ${Passwd} ${DBName} ${TableName} ${UserName} ${ModifiedTime} ${ProtoVersion}

if [ $? -ne 0 ]
then 
    echo "upload jar error, Please contact to administrator."
    exit 6
fi

rm -rf $HIVE_HOME/protobuf/upload/${UserName}/*.proto
