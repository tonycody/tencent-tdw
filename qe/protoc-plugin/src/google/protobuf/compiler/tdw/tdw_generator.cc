/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
/*
 * =====================================================================================
 *
 *       Filename:  tdw_generator.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 02:26:00 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#include <google/protobuf/compiler/tdw/tdw_generator.h>
#include <google/protobuf/compiler/tdw/tdw_file.h>
#include <google/protobuf/compiler/tdw/tdw_helpers.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

TdwGenerator::TdwGenerator() {}
TdwGenerator::~TdwGenerator() {}

bool TdwGenerator::Generate(const FileDescriptor* file,
                            const string& parameter,
                            OutputDirectory* output_directory,
                            string* error) const
{
  vector<pair<string, string> > options;
  ParseGeneratorParameter(parameter, &options);

  string database_name, table_name;
//added by cherry start
  string modified_time;
//added by cherry end
  for (size_t i = 0; i < options.size(); ++i ) {
    if (options[i].first == "database") {
      database_name = ToLower(options[i].second);
    } else if (options[i].first == "table") {
      table_name = ToLower(options[i].second);
    } 
//added by cherry start
  	 else if (options[i].first == "time") {
     modified_time = ToLower(options[i].second);
     }
//added by cherry end    
    else {
      *error = "Unknown generator option: " + options[i].first;
      return false;
    }
  }

  if (database_name.empty()) {
    *error = "Option missing: database";
    return false;
  }
  if (table_name.empty()) {
    *error = "Option missing: table";
    return false;
  }
//added by cherry start
  if (modified_time.empty()) {
    *error = "Option missing: modified_time";
    return false;
  }
//added by cherry end
  string filename = table_name;
  filename.append(".proto");
//modified by cherry start
//  FileGenerator file_generator(file, database_name, table_name);
  FileGenerator file_generator(file, database_name, table_name, modified_time);
//modified by cherry end
  if (!file_generator.Validate(error)) {
    return false;
  }
  scoped_ptr<io::ZeroCopyOutputStream> output(output_directory->Open(filename));
  io::Printer printer(output.get(), '$');
  file_generator.Generate(&printer);
  return true;
}

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

