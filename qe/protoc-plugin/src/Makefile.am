bin_PROGRAMS = protoc-gen-tdw
protoc_gen_tdw_CXXFLAGS = -I. -g -O2 -Wall
protoc_gen_tdw_SOURCES = \
	google/protobuf/compiler/tdw/plugin_main.cc \
	google/protobuf/compiler/tdw/tdw_dependent_file.cc \
	google/protobuf/compiler/tdw/tdw_field.cc \
	google/protobuf/compiler/tdw/tdw_file.cc \
	google/protobuf/compiler/tdw/tdw_generator.cc \
	google/protobuf/compiler/tdw/tdw_helpers.cc \
	google/protobuf/compiler/tdw/tdw_message.cc 

protoc_gen_tdw_LDADD = -lprotoc -lprotobuf -lpthread
