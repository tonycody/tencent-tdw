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
package org.apache.hadoop.hive.serde2.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.StringBuilder;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.CodedOutputStream;

public class ProtobufUtils {

  public static final Log LOG = LogFactory
      .getLog(ProtobufUtils.class.getName());

  public static Descriptor getMsgDescriptor(Class<?> msgClass) {
    try {
      Method getDescriptorMethod = msgClass.getMethod("getDescriptor",
          (Class<?>[]) null);
      Descriptor msgDescriptor = (Descriptor) getDescriptorMethod.invoke(null,
          (Object[]) null);
      return msgDescriptor;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static Class<?> loadTableMsgClass(String outerName, String name) {
    try {
      StringBuilder fullName = new StringBuilder();
      String pkgName = "tdw";
      fullName.append(pkgName);
      fullName.append(".");

      ArrayList<String> pieces = new ArrayList<String>();
      pieces.add(outerName);
      pieces.add(name);
      fullName.append(StringUtils.join(pieces, "$"));

      Class<?> cls = Class.forName(fullName.toString(), true,
          JavaUtils.getpbClassLoader());
      return cls;
    } catch (java.lang.ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static Class<?> loadFieldMsgClass(Descriptor msgDescriptor) {
    try {
      String fullName = getFieldMsgFullName(msgDescriptor);
      Class<?> cls = Class.forName(fullName, true, JavaUtils.getClassLoader());
      return cls;
    } catch (java.lang.ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static String getFieldMsgFullName(Descriptor msgDescriptor) {
    FileDescriptor fileDescriptor = msgDescriptor.getFile();

    StringBuilder fullName = new StringBuilder();
    assert (fileDescriptor.getOptions().hasJavaPackage());
    fullName.append(fileDescriptor.getOptions().getJavaPackage());
    fullName.append(".");

    ArrayList<String> pieces = new ArrayList<String>();
    Descriptor msg = msgDescriptor;
    while (true) {
      if (msg.getContainingType() == null) {
        if (!fileDescriptor.getOptions().hasJavaOuterClassname()) {
          assert (false);
        }
        String outerClassName = fileDescriptor.getOptions()
            .getJavaOuterClassname();
        pieces.add(0, msg.getName());
        pieces.add(0, outerClassName);
        break;
      } else {
        pieces.add(0, msg.getName());
        msg = msgDescriptor.getContainingType();
      }
    }
    fullName.append(StringUtils.join(pieces, "$"));
    return fullName.toString();
  }
}
