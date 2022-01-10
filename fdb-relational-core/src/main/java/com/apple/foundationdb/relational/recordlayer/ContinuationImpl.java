/*
 * ContinuationImpl.java
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
import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

class ContinuationImpl implements Continuation {
    private final byte[] continuationBytes;
    // extra state needed since the continuation comprises three states (begin, middle, end).
    private final boolean atEnd;

    // TODO(yhatem) remove semantic nulls.
    private ContinuationImpl(@Nullable  byte[] continuationBytes) {
        this(continuationBytes, false);
    }

    private ContinuationImpl(@Nullable byte[] continuationBytes, boolean atEnd) {
        this.continuationBytes = continuationBytes;
        this.atEnd = atEnd;
    }

    @Nullable
    @Override
    public byte[] getBytes() {
        return continuationBytes;
    }

    @Override
    public boolean atBeginning() {
        return !atEnd && this.continuationBytes == null;
    }

    public boolean atEnd() {
        return atEnd;
    }

    // factory methods.

    public static Continuation fromBytes(@Nullable byte[] bytes) {
        if (bytes == null) {
            return Continuation.END;
        }
        return new ContinuationImpl(Arrays.copyOf(bytes, bytes.length));
    }

    public static Continuation fromInt(int offset) {
        assert offset >= 0;
        return new ContinuationImpl(Ints.toByteArray(offset));
    }

    public static Continuation copyOf(@Nonnull Continuation other) {
        if (other instanceof ContinuationImpl) {
            return new ContinuationImpl(((ContinuationImpl) other).continuationBytes, ((ContinuationImpl) other).atEnd);
        } else if (other.atBeginning() && other.atEnd()) {
            return Continuation.EMPTY_SET;
        } else if (other.atBeginning()) {
            return Continuation.BEGIN;
        } else if (other.atEnd()) {
            return Continuation.END;
        } else {
            String message = String.format("programming error, extra logic required for copy-constructing from %s", other.getClass());
            assert false : message;
            throw new RelationalException(message, RelationalException.ErrorCode.INTERNAL_ERROR); // -ea safety net.
        }
    }
}
