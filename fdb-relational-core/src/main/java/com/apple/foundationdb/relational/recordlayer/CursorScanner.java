/*
 * CursorScanner.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.exceptions.InvalidCursorStateException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.function.Function;

public class CursorScanner<T> implements Scanner<KeyValue> {
    private RecordCursorIterator<T> cursor;
    private final Function<T, KeyValue> transform;
    private boolean supportsMessageParsing;

    private CursorScanner(RecordCursor<T> cursor, Function<T, KeyValue> transform, boolean supportsMessageParsing) {
        this.cursor = cursor.asIterator();
        this.transform = transform;
        this.supportsMessageParsing = supportsMessageParsing;
    }

    public static <T> CursorScanner<T> create(RecordCursor<T> cursor, Function<T, KeyValue> transform, boolean supportsMessageParsing) {
        return new CursorScanner<>(cursor, transform, supportsMessageParsing);
    }

    @Override
    public void close() throws RelationalException {
        cursor.close();
    }

    @Override
    public Continuation continuation() {
        byte[] cont = cursor.getContinuation();
        //TODO(bfines) replace this with mutable abstraction?
        return new RecordLayerContinuation(cont);
    }

    @Override
    public boolean supportsMessageParsing() {
        return supportsMessageParsing;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public KeyValue next() {
        if (!hasNext()) {
            throw new InvalidCursorStateException("No next element with the cursor");
        }
        return transform.apply(cursor.next());
    }
}
