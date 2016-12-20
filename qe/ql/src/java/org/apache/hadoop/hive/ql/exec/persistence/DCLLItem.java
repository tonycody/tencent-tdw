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

package org.apache.hadoop.hive.ql.exec.persistence;

public class DCLLItem {

  DCLLItem prev;
  DCLLItem next;

  DCLLItem() {
    prev = next = this;
  }

  public DCLLItem getNext() {
    return next;
  }

  public DCLLItem getPrev() {
    return prev;
  }

  public void setNext(DCLLItem itm) {
    next = itm;
  }

  public void setPrev(DCLLItem itm) {
    prev = itm;
  }

  public void remove() {
    next.prev = this.prev;
    prev.next = this.next;
    this.prev = this.next = null;
  }

  public void insertBefore(DCLLItem v) {
    this.prev.next = v;
    v.prev = this.prev;
    v.next = this;
    this.prev = v;
  }

  public void insertAfter(DCLLItem v) {
    this.next.prev = v;
    v.next = this.next;
    v.prev = this;
    this.next = v;
  }
}
