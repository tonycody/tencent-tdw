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

package org.apache.hadoop.hive.ant;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.Project;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.*;

public class GetVersionPref extends Task {

  protected String property;

  protected String input;

  public void setProperty(String property) {
    this.property = property;
  }

  public String getProperty() {
    return property;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getInput() {
    return input;
  }

  @Override
  public void execute() throws BuildException {

    if (property == null) {
      throw new BuildException("No property specified");
    }

    if (input == null) {
      throw new BuildException("No input stringspecified");
    }

    try {
      Pattern p = Pattern.compile("^(\\d+\\.\\d+).*");
      Matcher m = p.matcher(input);
      getProject().setProperty(property, m.matches() ? m.group(1) : "");
    } catch (Exception e) {
      throw new BuildException("Failed with: " + e.getMessage());
    }
  }
}
