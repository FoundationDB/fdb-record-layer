/*
 * BloomFilterCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.RecordCursorContinuation;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.apple.foundationdb.record.RecordCursorProto.ProbableIntersectionContinuation;

class BloomFilterCursorContinuation implements RecordCursorContinuation {
    @Nonnull
    private final RecordCursorContinuation childContinuation;
    @Nullable
    private final ByteString bloomBytes;
    @Nullable
    private ProbableIntersectionContinuation.CursorState cachedProto;
    @Nullable
    private byte[] cachedBytes;
    @Nullable
    private ByteString cachedByteString;

    BloomFilterCursorContinuation(@Nonnull RecordCursorContinuation childContinuation, @Nullable ByteString bloomBytes) {
        this.childContinuation = childContinuation;
        this.bloomBytes = bloomBytes;
    }

    @Nonnull
    ProbableIntersectionContinuation.CursorState toProto() {
        if (cachedProto == null) {
            ProbableIntersectionContinuation.CursorState.Builder builder = ProbableIntersectionContinuation.CursorState.newBuilder();
            if (childContinuation.isEnd()) {
                builder.setExhausted(true);
            } else {
                byte[] childBytes = childContinuation.toBytes();
                if (childBytes != null) {
                    builder.setContinuation(ByteString.copyFrom(childBytes));
                }
            }
            if (bloomBytes != null) {
                builder.setBloomFilter(bloomBytes);
            }
            cachedProto = builder.build();
        }
        return cachedProto;
    }

    @Override
    @Nullable
    public byte[] toBytes() {
        if (cachedBytes == null) {
            cachedBytes = toByteString().toByteArray();
        }
        return cachedBytes;
    }

    @Override
    @Nonnull
    public ByteString toByteString() {
        if (cachedByteString == null) {
            cachedByteString = toProto().toByteString();
        }
        return cachedByteString;
    }

    @Nonnull
    RecordCursorContinuation getChild() {
        return childContinuation;
    }

    @Nullable
    ByteString getBloomBytes() {
        return bloomBytes;
    }

    // Bloom continuations can never themselves be end continuations, though their children might be.
    @Override
    public boolean isEnd() {
        return false;
    }

    boolean isChildEnd() {
        return childContinuation.isEnd();
    }
}
