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
 *       Filename:  tdw_message.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 05:56:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#include <google/protobuf/compiler/tdw/tdw_message.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/compiler/tdw/tdw_field.h>
#include <google/protobuf/compiler/tdw/tdw_helpers.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

MessageGenerator::MessageGenerator(const Descriptor* message, bool in_dependency)
  : message_(message),
    table_name_(""),
    in_dependency_(in_dependency),
    nested_messages_(GetNestedMessages(message)),
    nested_generators_(
        new scoped_ptr<MessageGenerator>[nested_messages_.size()]),
    field_generators_(
        new scoped_ptr<FieldGenerator>[message->field_count()])
{
  for (size_t i = 0; i < nested_messages_.size(); i++) {
    nested_generators_[i].reset(
        new MessageGenerator(nested_messages_[i], in_dependency));
  }
  for (int i = 0; i < message->field_count(); i++) {
    field_generators_[i].reset(
        new FieldGenerator(message->field(i), in_dependency));
  }
}

MessageGenerator::MessageGenerator(const Descriptor* message, const string& table_name)
  : message_(message),
    table_name_(table_name),
    in_dependency_(false),
    nested_messages_(GetNestedMessages(message)),
    nested_generators_(
        new scoped_ptr<MessageGenerator>[nested_messages_.size()]),
    field_generators_(
        new scoped_ptr<FieldGenerator>[message->field_count()])
{
  for (size_t i = 0; i < nested_messages_.size(); i++) {
    nested_generators_[i].reset(
        new MessageGenerator(nested_messages_[i], in_dependency_));
  }
  for (int i = 0; i < message->field_count(); i++) {
    field_generators_[i].reset(
        new FieldGenerator(message->field(i), in_dependency_));
  }
}

MessageGenerator::~MessageGenerator() {}

void MessageGenerator::Generate(io::Printer* printer) {
  GenerateHead(printer);
  GenerateBody(printer);
}

void MessageGenerator::GenerateHead(io::Printer* printer) {
  string name = message_->name();
  if (!message_->containing_type() && in_dependency_) {
    name = GetDependentMessageName(message_);
  }
  
  if (ToLower(name) == table_name_) {
      printer->Print(
          "message $name$",
          "name", ToLower(name));
  }
  else{
      printer->Print(
          "message $name$",
          "name", name);
  }
}

void MessageGenerator::GenerateBody(io::Printer* printer) {
  printer->Print(
      " {\n");
  printer->Indent();

  for (size_t i=0; i<nested_messages_.size(); i++) {
    nested_generators_[i]->Generate(printer);
  }
  for (int i=0; i<message_->field_count(); i++) {
    field_generators_[i]->Generate(printer);
  }

  printer->Outdent();
  printer->Print(
      "}\n");
}

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

