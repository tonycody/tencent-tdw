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

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.io.PrintStream;
import java.io.PrintWriter;

public class WrappedRuntimeException extends RuntimeException {

  private final Exception _except;

  public WrappedRuntimeException(String message, Exception except) {
    super(message == null ? "No message available" : message);

    if (except instanceof WrappedRuntimeException
        && ((WrappedRuntimeException) except)._except != null) {
      _except = ((WrappedRuntimeException) except)._except;
    } else {
      _except = except;
    }
  }

  public WrappedRuntimeException(Exception except) {
    super(
        except == null || except.getMessage() == null ? "No message available"
            : except.getMessage());

    if (except instanceof WrappedRuntimeException
        && ((WrappedRuntimeException) except)._except != null) {
      _except = ((WrappedRuntimeException) except)._except;
    } else {
      _except = except;
    }
  }

  public Exception getException() {
    return _except;
  }

  public void printStackTrace() {
    if (_except == null) {
      super.printStackTrace();
    } else {
      _except.printStackTrace();
    }
  }

  public void printStackTrace(PrintStream stream) {
    if (_except == null) {
      super.printStackTrace(stream);
    } else {
      _except.printStackTrace(stream);
    }
  }

  public void printStackTrace(PrintWriter writer) {
    if (_except == null) {
      super.printStackTrace(writer);
    } else {
      _except.printStackTrace(writer);
    }
  }

}
