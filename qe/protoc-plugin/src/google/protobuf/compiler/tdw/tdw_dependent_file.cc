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
 *       Filename:  tdw_dependent_file.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 05:52:05 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */


#include <google/protobuf/compiler/tdw/tdw_dependent_file.h>
#include <google/protobuf/compiler/tdw/tdw_message.h>
#include <google/protobuf/compiler/tdw/tdw_helpers.h>
#include <google/protobuf/io/printer.h>


namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

DependentFileGenerator::DependentFileGenerator(const FileDescriptor* file, 
    const FileDescriptor* parent)
  : file_(file),
    parent_(parent),
    message_generators_(
        new scoped_ptr<MessageGenerator>[file->message_type_count()])
{
  for (int i=0; i<file->message_type_count(); i++) {
    message_generators_[i].reset(
        new MessageGenerator(file->message_type(i), true));
  }
}

DependentFileGenerator::~DependentFileGenerator() {}

void DependentFileGenerator::Generate(io::Printer* printer) {
  printer->Print("\n");
  printer->Print(kThickSeparator);
  printer->Print(
      "// Begin dependent file: $filename$\n",
      "filename", file_->name());

  for (int i=0; i<file_->message_type_count(); i++) {
    if (i>1) {
      printer->Print("\n");
    }
    message_generators_[i]->Generate(printer);
  }

  printer->Print(
      "// End dependent file: $filename$\n",
      "filename", file_->name());
  printer->Print(kThickSeparator);
}

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

