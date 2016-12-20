#!/bin/bash

cd $(dirname $0)
mkdir -p ../../build/dist/protobuf/upload/root
cp ./person.proto ../../build/dist/protobuf/upload/root/
../../build/dist/bin/makejar.sh jdbc:postgresql://localhost:5432/pbjar tdwmeta tdwmeta default_db person root person.proto 2.5.0
