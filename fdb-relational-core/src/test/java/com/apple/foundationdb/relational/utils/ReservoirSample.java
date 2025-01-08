/*
 * ReservoirSample.java
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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A simple Reservoir Sample of Objects. Used to keep a sample of data during load operations in comparison
 * tests.
 *
 * @param <T> the type of data held in the sample.
 */
public class ReservoirSample<T> {
    private final Object[] data;
    private final Random random;
    private int size;

    public ReservoirSample(int size, long randomSeed) {
        this(size, new Random(randomSeed));
    }

    public ReservoirSample(int size, Random random) {
        this.random = random;
        this.data = new Object[size];
    }

    public void add(T item) {
        if (size < data.length) {
            data[size] = item;
            size++;
        } else {
            int pos = random.nextInt(size);
            if (pos < data.length) {
                data[pos] = item;
            }
        }
    }

    public Iterator<T> sampleIterator() {
        return new Iterator<>() {
            int position = 0;

            @Override
            public boolean hasNext() {
                return position < Math.min(size, data.length);
            }

            @Override
            @SuppressWarnings("unchecked")
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                T n = (T) data[position];
                position++;
                return n;
            }
        };
    }

    public Stream<T> sampleStream() {
        Spliterator<T> spliterator = Spliterators.spliterator(sampleIterator(), size, Spliterator.SIZED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }

}
