/*
 * ResolverResult.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import org.jspecify.annotations.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A class containing the result of resolving a key with {@link LocatableResolver}.
 */
@API(API.Status.UNSTABLE)
public class ResolverResult {
    private final long value;
    @Nullable
    private final byte[] metadata;

    // NullAway/JSpecify does not currently track @Nullable on array (byte[]) parameters, even
    // for a null literal passed to another constructor that declares the same array @Nullable.
    @SuppressWarnings("NullAway")
    public ResolverResult(long value) {
        this(value, null);
    }

    // NullAway/JSpecify does not currently track @Nullable on array (byte[]) fields; metadata
    // is correctly assigned null when the given metadata is null.
    @SuppressWarnings("NullAway")
    public ResolverResult(long value, @Nullable byte[] metadata) {
        this.value = value;
        this.metadata = metadata == null ? null : Arrays.copyOf(metadata, metadata.length);
    }

    public long getValue() {
        return value;
    }

    @Nullable
    @SuppressWarnings("NullAway") // NullAway/JSpecify does not currently track @Nullable on array (byte[]) return types.
    public byte[] getMetadata() {
        return metadata == null ? null : Arrays.copyOf(metadata, metadata.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ResolverResult) {
            ResolverResult that = (ResolverResult) obj;
            return (this.value == that.value) && Arrays.equals(this.metadata, that.metadata);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(value);
        return 29 * result + 31 * Arrays.hashCode(metadata);
    }

    @Override
    // NullAway does not reliably track @Nullable on the byte[] metadata field across this call into
    // ByteArrayUtil2.loggable, even though loggable's own parameter is declared @Nullable byte[].
    @SuppressWarnings("NullAway")
    public String toString() {
        return "Value: " + value  + ", metadata: " + ByteArrayUtil2.loggable(metadata);
    }
}
