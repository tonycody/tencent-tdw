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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MinHeap<T> implements Iterable<T> {
  Comparator<T> c;
  List<T> items = new ArrayList<T>();

  public MinHeap(Comparator<T> c) {
    this.c = c;
    items.add(null);
  }

  public void add(T item) {
    items.add(item);
    buildMinHeap();
  }

  public void remove(T item) {
    items.remove(item);
    buildMinHeap();
  }

  public T top() {
    return items.get(1);
  }

  public boolean isEmpty() {
    return getNumberOfItems() <= 0;
  }

  public T pop() {
    if (getNumberOfItems() <= 0) {
      throw new IllegalStateException("heap empty");
    }

    T item = items.get(1);

    items.remove(item);

    buildMinHeap();

    return item;
  }

  public int getNumberOfItems() {
    return items.size() - 1;
  }

  public int getHeight() {
    return getHeight(1);
  }

  private int getHeight(int i) {
    if (i >= items.size()) {
      return 0;
    }

    final int left = i * 2;
    final int right = left + 1;

    return Math.max(getHeight(left), getHeight(right)) + 1;
  }

  private void buildMinHeap() {
    for (int i = getNumberOfItems() / 2; i >= 1; i--) {
      minHeapify(i);
    }
  }

  private void minHeapify(final int i) {
    if (i >= 1 && i < items.size()) {
      final int left = i * 2;
      final int right = left + 1;

      int max = i;

      if (left < items.size()) {
        final T ileft = items.get(left);
        final T mi = items.get(max);
        if (c.compare(mi, ileft) > 0) {
          max = left;
        }
      }

      if (right < items.size()) {
        final T iright = items.get(right);
        final T mi = items.get(max);
        if (c.compare(mi, iright) > 0) {
          max = right;
        }
      }

      if (max != i) {
        final T mi = items.get(max);
        final T ii = items.get(i);

        items.set(i, mi);
        items.set(max, ii);

        minHeapify(max);
      }
    }
  }

  public boolean contains(T item) {
    return items.contains(item);
  }

  public T[] toArray(T[] array) {
    return items.subList(1, items.size()).toArray(array);
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      int index = 1;
      List<T> items = new ArrayList<T>(MinHeap.this.items);

      @Override
      public boolean hasNext() {
        return index < items.size();
      }

      @Override
      public T next() {
        return items.get(index++);
      }

      @Override
      public void remove() {
        items.remove(index);
      }

    };
  }
}
