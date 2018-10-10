/*
 * ListCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A cursor that returns the elements of a list.
 * @param <T> the type of elements of the cursor
 */
public class ListCursor<T> extends IteratorCursor<T> {
    private int position;

    public ListCursor(@Nonnull List<T> list, byte []continuation) {
        super(ForkJoinPool.commonPool(), list.iterator());
        if (continuation != null) {
            position = ByteBuffer.wrap(continuation).getInt();
            for (int i = 0; i < position; i++) {
                iterator.next();
            }
        }
    }

    public ListCursor(@Nonnull Executor executor, @Nonnull List<T> list, int position) {
        super(executor, list.iterator());
        this.position = position;
    }

    @Nullable
    @Override
    public T next() {
        T next = super.next();
        if (iterator.hasNext()) {
            position++;
        } else {
            position = -1;
        }
        return next;
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        if (position < 0) {
            return null;
        } else {
            return ByteBuffer.allocate(Integer.BYTES).putInt(position).array();
        }
    }
}
