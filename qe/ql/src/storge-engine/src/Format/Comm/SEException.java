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
package Comm;

public class SEException {
  @SuppressWarnings("serial")
  static public class UnitFullException extends Exception {
    public UnitFullException(String s) {
      super(s);
    }

    public UnitFullException(Throwable cause) {
      super(cause);
    }
  }

  static public class SegmentFullException extends Exception {
    public SegmentFullException(String s) {
      super(s);
    }

    public SegmentFullException(Throwable cause) {
      super(cause);
    }
  }

  static public class InvalidParameterException extends Exception {
    public InvalidParameterException(String s) {
      super(s);
    }

    public InvalidParameterException(Throwable cause) {
      super(cause);
    }
  }

  static public class ErrorFileFormat extends Exception {
    public ErrorFileFormat(String s) {
      super(s);
    }

    public ErrorFileFormat(Throwable cause) {
      super(cause);
    }
  }

  static public class FieldValueFull extends Exception {
    public FieldValueFull(String s) {
      super(s);
    }

    public FieldValueFull(Throwable cause) {
      super(cause);
    }
  }

  static public class MaxRecordLimitedException extends Exception {
    public MaxRecordLimitedException(String s) {
      super(s);
    }

    public MaxRecordLimitedException(Throwable cause) {
      super(cause);
    }
  }

  static public class NotSupportException extends Exception {
    public NotSupportException(String s) {
      super(s);
    }

    public NotSupportException(Throwable cause) {
      super(cause);
    }
  }

  static public class NoEntityException extends Exception {
    public NoEntityException(String s) {
      super(s);
    }

    public NoEntityException(Throwable cause) {
      super(cause);
    }
  }

  static public class InnerException extends Exception {
    public InnerException(String s) {
      super(s);
    }

    public InnerException(Throwable cause) {
      super(cause);
    }
  }
}
