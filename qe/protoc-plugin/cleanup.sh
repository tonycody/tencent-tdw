#!/bin/bash

if [ -f "Makefile" ]; then
  make clean
  rm -f Makefile src/Makefile
fi

if [ -f "src/protoc-gen-tdw" ]; then
  rm -f src/protoc-gen-tdw src/*.o
fi

rm -f Makefile.in src/Makefile.in
rm -rf .deps src/.deps autom4te.cache
rm -f aclocal.m4 configure depcomp install-sh missing config.log config.status
