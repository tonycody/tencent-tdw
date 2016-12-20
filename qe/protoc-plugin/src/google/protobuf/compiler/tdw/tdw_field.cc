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
 *       Filename:  tdw_field.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 06:16:55 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#include <google/protobuf/compiler/tdw/tdw_field.h>
#include <google/protobuf/compiler/tdw/tdw_helpers.h>
#include <google/protobuf/compiler/tdw/tdw_message.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/descriptor.pb.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

FieldGenerator::FieldGenerator(const FieldDescriptor* field, bool in_dependency)
  : field_(field),
    in_dependency_(in_dependency)
{
  InitVariables();
}

FieldGenerator::~FieldGenerator() {}

void FieldGenerator::InitVariables() {
  FieldDescriptor::Type type = field_->type();
  string type_string;
  string tag = SimpleItoa(field_->number());
  string field_name = ToLower(field_->name());
  switch (type) {
    case FieldDescriptor::TYPE_MESSAGE:
      type_string = GetFieldMessageName(field_->message_type(), field_->containing_type(), in_dependency_);
      break;
    case FieldDescriptor::TYPE_ENUM:
      type_string = ToLower(kTypeToName[FieldDescriptor::TYPE_INT32]);
      break;
    case FieldDescriptor::TYPE_BYTES:
      // 'bytes' is transformed to 'string' type.
      type_string = ToLower(kTypeToName[FieldDescriptor::TYPE_STRING]);
      break;
    case FieldDescriptor::TYPE_GROUP:
      type_string = ToLower(kTypeToName[type]);
      field_name = ToGroupName(field_name);
      break;
    default:
      type_string = kTypeToName[type];
      break;
  }

  variables_["label"] = kLabelToName[field_->label()];
  variables_["type"] = type_string;
  variables_["name"] = field_name;
  variables_["tag"] = tag;
}

void FieldGenerator::Generate(io::Printer* printer) {
  printer->Print(variables_,
      "$label$ $type$ $name$ = $tag$ ");

  vector<string> option_list;
  const FieldOptions& options = field_->options();
  if (options.packed()) {
    option_list.push_back("packed=true");
  }
  if (options.has_ctype()) {
    option_list.push_back("ctype=" + FieldOptions::CType_Name(options.ctype()));
  }
  if (options.deprecated()) {
    option_list.push_back("deprecated=true");
  }
  if (field_->has_default_value()) {
    string default_value = DefaultValue(field_);
    option_list.push_back("default=" + default_value);
  }
  if (!option_list.empty()) {
    string option_string = JoinStrings(option_list, ",");
    printer->Print(
        "[ $option_string$ ] ",
        "option_string", option_string);
  }
  
  FieldDescriptor::Type type = field_->type();
  if (type == FieldDescriptor::TYPE_GROUP) {
    MessageGenerator group_generator(field_->message_type(), in_dependency_);
    group_generator.GenerateBody(printer);
  } else {
    printer->Print(";\n");
  }
}

/*
 * Copy from descriptor.cc
 */
const char * const FieldGenerator::kLabelToName[FieldDescriptor::MAX_LABEL + 1] = {                                                                                                                                       
  "ERROR",     // 0 is reserved for errors
  "optional",  // LABEL_OPTIONAL
  "required",  // LABEL_REQUIRED
  "repeated",  // LABEL_REPEATED
};

const char * const FieldGenerator::kTypeToName[FieldDescriptor::MAX_TYPE + 1] = {                                                                                                                                         
  "ERROR",     // 0 is reserved for errors
  "double",    // TYPE_DOUBLE
  "float",     // TYPE_FLOAT   
  "int64",     // TYPE_INT64   
  "uint64",    // TYPE_UINT64  
  "int32",     // TYPE_INT32   
  "fixed64",   // TYPE_FIXED64                        
  "fixed32",   // TYPE_FIXED32 
  "bool",      // TYPE_BOOL
  "string",    // TYPE_STRING
  "group",     // TYPE_GROUP
  "message",   // TYPE_MESSAGE
  "bytes",     // TYPE_BYTES
  "uint32",    // TYPE_UINT32    
  "enum",      // TYPE_ENUM
  "sfixed32",  // TYPE_SFIXED32
  "sfixed64",  // TYPE_SFIXED64
  "sint32",    // TYPE_SINT32
  "sint64",    // TYPE_SINT64
};

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

