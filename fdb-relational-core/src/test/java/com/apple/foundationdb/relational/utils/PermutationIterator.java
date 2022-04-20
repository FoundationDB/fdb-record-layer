/*
 * PermutationIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.relational.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

public class PermutationIterator<T> implements Iterator<List<T>> {
    private final List<T> items;
    @Nullable
    PermutationIterator<T> next;

    int currentPosition;

    boolean nextCalled;
    List<T> currentRow;

    public static <T> PermutationIterator<T> generatePermutations(List<T> items, int numColumns) {
        PermutationIterator<T> iter = new PermutationIterator<>(items);
        PermutationIterator<T> tail = iter;
        for (int i = 0; i < numColumns - 1; i++) {
            tail = new PermutationIterator<>(items, tail);
        }
        return iter;
    }

    public PermutationIterator(List<T> items) {
        this.items = items;
    }

    public PermutationIterator(@Nullable List<T> items, @Nullable PermutationIterator<T> parent) {
        this.items = items;
        parent.next = this;
    }

    public Stream<List<T>> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.SIZED), false);
    }

    @Override
    public boolean hasNext() {
        if (nextCalled) {
            return currentRow != null;
        }

        nextCalled = true;
        List<T> nextElement = new ArrayList<>();
        if (insertNext(nextElement)) {
            currentRow = nextElement;
        }
        return currentRow != null;
    }

    @Override
    public List<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        List<T> ret = currentRow;
        nextCalled = false;
        currentRow = null;
        return ret;
    }

    public boolean insertNext(List<T> destination) {
        if (currentPosition >= items.size()) {
            return false;
        }
        T item = items.get(currentPosition);
        if (next == null) {
            destination.add(item);
            currentPosition++;
            return true;
        } else {
            int listPos = destination.size();
            if (!next.insertNext(destination)) {
                next.reset();
                currentPosition++;
                return insertNext(destination);
            } else {
                destination.add(listPos, item);
                return true;
            }
        }
    }

    private void reset() {
        currentPosition = 0;
    }
}
