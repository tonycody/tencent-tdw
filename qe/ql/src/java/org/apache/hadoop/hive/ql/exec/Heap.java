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
package org.apache.hadoop.hive.ql.exec;

import java.util.Collection;
import java.util.Comparator;
import java.util.ArrayList;

public class Heap<T> extends ArrayList<T> {
  private Comparator comp = null;

  protected boolean isHeap = true;

  public Heap() {
  }

  public Heap(Comparator c) {
    comp = c;
  }

  public Heap(int capacity) {
    super(capacity);
  }

  public Heap(Collection<? extends T> c) {
    addAll(c);
  }

  public boolean add(T v) {
    if (!isHeap) {
      makeHeap();
    }
    boolean b = super.add(null);
    __push(0, size() - 1, v);
    return b;
  }

  public T remove(int index) {
    if (!isHeap) {
      makeHeap();
    }
    if (index == size() - 1) {
      return super.remove(index);
    } else {
      T r = get(index);
      T v = super.remove(size() - 1);
      __adjust(index, v);
      return r;
    }
  }

  public boolean remove(Object v) {
    boolean found = false;
    for (int i = 0; i < size(); i++) {
      if (v == null ? get(i) == null : v.equals(get(i))) {
        found = true;
        super.remove(i);
        break;
      }
    }
    return found;
  }

  public boolean addAll(Collection<? extends T> c) {
    boolean b = super.addAll(c);
    makeHeap();
    return b;
  }

  public void makeHeap() {
    if (size() < 2) {
      return;
    }
    int parent = (size() - 2) / 2;
    while (true) {
      __adjust(parent, get(parent));
      if (parent == 0) {
        break;
      }
      parent--;
    }
    isHeap = true;
  }

  public void sort() {
    if (!isHeap) {
      return;
    }
    int size = size();
    while (--size > 0) {
      T v = get(size);
      set(size, get(0));
      __adjust(0, v, size);
    }
    isHeap = false;
  }

  protected void __push(int top, int hole, T v) {
    int parent = (hole - 1) / 2;
    while (hole > top && __cmp(get(parent), v) < 0) {
      set(hole, get(parent));
      hole = parent;
      parent = (hole - 1) / 2;
    }
    set(hole, v);
  }

  protected void __adjust(int hole, T v, int size) {
    int top = hole;
    int child2 = 2 * hole + 2;
    while (child2 < size) {
      if (__cmp(get(child2), get(child2 - 1)) < 0) {
        child2--;
      }
      set(hole, get(child2));
      hole = child2;
      child2 = 2 * child2 + 2;
    }
    if (child2 == size) {
      set(hole, get(child2 - 1));
      hole = child2 - 1;
    }
    __push(top, hole, v);
  }

  protected void __adjust(int hole, T v) {
    __adjust(hole, v, size());
  }

  @SuppressWarnings("unchecked")
  protected int __cmp(T v1, T v2) {
    if (comp != null) {
      return comp.compare(v1, v2);
    } else {
      return ((Comparable) v1).compareTo(v2);
    }
  }

  public static void main(String[] argv) {
    Heap<Integer> h = new Heap<Integer>();
    h.add(3);
    h.add(2);
    h.add(9);
    h.add(6);
    h.add(7);
    h.add(4);
    h.add(1);
    h.add(5);
    h.add(8);
    System.out.println("head is: " + h.get(0));
    h.sort();
    for (int i = 0; i < h.size(); i++) {
      System.out.print(h.get(i) + " ");
    }
    System.out.println();
  }
}
