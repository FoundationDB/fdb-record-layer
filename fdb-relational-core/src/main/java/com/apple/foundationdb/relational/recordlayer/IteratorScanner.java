/*
 * IteratorScanner.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * A Scanner which moves over an Iterator.
 *
 * @param <T> the type of the Scanner
 */
public class IteratorScanner<T> implements Scanner<T> {
    private final Iterator<T> iterator;
    private final boolean supportsMessageParsing;

    public IteratorScanner(@Nonnull Iterator<T> iterator, boolean supportsMessageParsing) {
        this.iterator = iterator;
        this.supportsMessageParsing = supportsMessageParsing;
    }

    @Override
    public void close() throws RelationalException {
        //no-op
    }

    @Override
    public Continuation continuation() {
        return null;
    }

    @Override
    public boolean supportsMessageParsing() {
        return supportsMessageParsing;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }
}
