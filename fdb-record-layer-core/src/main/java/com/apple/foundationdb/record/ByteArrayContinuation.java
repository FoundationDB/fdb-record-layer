/*
 * ByteArrayContinuation.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.annotation.API;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import org.jspecify.annotations.Nullable;
import java.nio.ByteBuffer;

/**
 * A shim to wrap old-style byte array continuations.
 */
@API(API.Status.EXPERIMENTAL)
public class ByteArrayContinuation implements RecordCursorContinuation {
    private final byte[] bytes;
    @Nullable
    private ByteString byteString;

    private ByteArrayContinuation(final byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    @SpotBugsSuppressWarnings("EI")
    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public ByteString toByteString() {
        if (byteString == null) {
            byteString = ZeroCopyByteString.wrap(bytes);
        }
        return byteString;
    }

    @Override
    public boolean isEnd() {
        return false;
    }

    /**
     * Return a {@code RecordCursorContinuation} from a byte array. If the given byte array is non-null, return a
     * {@code ByteArrayContinuation} wrapping that array. If the given byte array is {@code null}, return a
     * {@link RecordCursorEndContinuation} instead.
     * @param bytes a nullable byte array representing a serialized continuation
     * @return a {@code RecordCursorContinuation} as described above
     */
    public static RecordCursorContinuation fromNullable(@Nullable final byte[] bytes) {
        if (bytes == null) {
            return RecordCursorEndContinuation.END;
        } else {
            return new ByteArrayContinuation(bytes);
        }
    }

    /**
     * Return a {@code RecordCursorContinuation} that wraps a byte array consisting of the given integer.
     * @param a an integer
     * @return a {@code ByteArrayContinuation} wrapping a byte array of the given integer
     */
    public static RecordCursorContinuation fromInt(int a) {
        return new ByteArrayContinuation(ByteBuffer.allocate(Integer.BYTES).putInt(a).array());
    }
}
