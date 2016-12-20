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
 *       Filename:  tdw_field.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 06:16:17 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#ifndef TDW_FIELD_H__
#define TDW_FIELD_H__

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>

namespace google {                                                                                   
namespace protobuf {                                                                                 
  namespace io {                                                                                     
    class Printer;             // printer.h                                                          
  }                                                                                                  
}

namespace protobuf {
namespace compiler {
namespace tdw {

class FieldGenerator {
public:
  FieldGenerator(const FieldDescriptor* field, bool in_dependency);
  ~FieldGenerator();

  void Generate(io::Printer* printer);
private:
  void InitVariables();

  const FieldDescriptor* field_;
  bool in_dependency_;
  map<string, string> variables_;
  GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(FieldGenerator);

  static const char * const kLabelToName[FieldDescriptor::MAX_LABEL + 1];
  static const char * const kTypeToName[FieldDescriptor::MAX_TYPE + 1]; 
};

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google


#endif // TDW_FIELD_H__

