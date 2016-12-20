/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

@description(name = "like", value = "_FUNC_(str, pattern) - Checks if str matches pattern", extended = "Example:\n"
    + "  > SELECT a.* FROM srcpart a WHERE a.hr _FUNC_ '%2' LIMIT 1;\n"
    + "  27      val_27  2008-04-08      12")
public class UDFLike extends UDF {

  private static Log LOG = LogFactory.getLog(UDFLike.class.getName());
  private Text lastLikePattern = new Text();
  private Pattern p = null;

  private enum PatternType {
    NONE, BEGIN, END, MIDDLE, COMPLEX,
  }

  private PatternType type = PatternType.COMPLEX;
  private Text simplePattern = new Text();

  private BooleanWritable result = new BooleanWritable();

  public UDFLike() {
  }

  public static String likePatternToRegExp(String likePattern) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < likePattern.length(); i++) {
      char n = likePattern.charAt(i);
      if (n == '\\'
          && i + 1 < likePattern.length()
          && (likePattern.charAt(i + 1) == '_' || likePattern.charAt(i + 1) == '%')) {
        sb.append(likePattern.charAt(i + 1));
        i++;
        continue;
      }

      if (n == '_') {
        sb.append(".");
      } else if (n == '%') {
        sb.append(".*");
      } else {
        if ("\\[](){}.*^$".indexOf(n) != -1) {
          sb.append('\\');
        }
        sb.append(n);
      }
    }
    return sb.toString();
  }

  private void parseSimplePattern(String likePattern) {
    int length = likePattern.length();
    int beginIndex = 0;
    int endIndex = length;
    char lastChar = 'a';
    String strPattern = new String();
    type = PatternType.NONE;

    for (int i = 0; i < length; i++) {
      char n = likePattern.charAt(i);
      if (n == '_') {
        if (lastChar != '\\') {
          type = PatternType.COMPLEX;
          return;
        } else {
          strPattern += likePattern.substring(beginIndex, i - 1);
          beginIndex = i;
        }
      } else if (n == '%') {
        if (i == 0) {
          type = PatternType.END;
          beginIndex = 1;
        } else if (i < length - 1) {
          if (lastChar != '\\') {
            type = PatternType.COMPLEX;
            return;
          } else {
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
          }
        } else {
          if (lastChar != '\\') {
            endIndex = length - 1;
            if (type == PatternType.END) {
              type = PatternType.MIDDLE;
            } else {
              type = PatternType.BEGIN;
            }
          } else {
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
            endIndex = length;
          }
        }
      }
      lastChar = n;
    }

    strPattern += likePattern.substring(beginIndex, endIndex);
    simplePattern.set(strPattern);
  }

  private static boolean find(Text s, Text sub, int startS, int endS) {
    byte[] byteS = s.getBytes();
    byte[] byteSub = sub.getBytes();
    int lenSub = sub.getLength();
    boolean match = false;
    for (int i = startS; (i < endS - lenSub + 1) && (!match); i++) {
      match = true;
      for (int j = 0; j < lenSub; j++) {
        if (byteS[j + i] != byteSub[j]) {
          match = false;
          break;
        }
      }
    }
    return match;
  }

  public BooleanWritable evaluate(Text s, Text likePattern) {
    if (s == null || likePattern == null) {
      return null;
    }
    if (!likePattern.equals(lastLikePattern)) {
      lastLikePattern.set(likePattern);
      String strLikePattern = likePattern.toString();

      parseSimplePattern(strLikePattern);
      if (type == PatternType.COMPLEX)
        p = Pattern.compile(likePatternToRegExp(strLikePattern));
    }

    if (type == PatternType.COMPLEX) {
      Matcher m = p.matcher(s.toString());
      result.set(m.matches());
    } else {
      int startS = 0;
      int endS = s.getLength();
      if (endS < simplePattern.getLength()) {
        result.set(false);
        return result;
      }
      switch (type) {
      case BEGIN:
        endS = simplePattern.getLength();
        break;
      case END:
        startS = endS - simplePattern.getLength();
        break;
      case NONE:
        if (simplePattern.getLength() != s.getLength()) {
          result.set(false);
          return result;
        }
        break;
      }
      result.set(find(s, simplePattern, startS, endS));
    }
    return result;
  }

}
