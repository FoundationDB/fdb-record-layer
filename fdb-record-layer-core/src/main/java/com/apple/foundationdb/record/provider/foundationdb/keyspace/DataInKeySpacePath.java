/*
 * DataInKeySpacePath.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Class representing a {@link KeyValue} pair within in {@link KeySpacePath}.
 */
@API(API.Status.EXPERIMENTAL)
public class DataInKeySpacePath {

    @Nonnull
    private final KeySpacePath path;
    @Nullable
    private final Tuple remainder;
    @Nonnull
    private final ByteString value;

    public DataInKeySpacePath(@Nonnull final KeySpacePath path, @Nullable final Tuple remainder,
                              @Nullable final byte[] value) {
        this.path = path;
        this.remainder = remainder;
        if (value == null) {
            throw new RecordCoreArgumentException("Value cannot be null")
                    .addLogInfo(LogMessageKeys.KEY, path);
        }
        this.value = ByteString.copyFrom(value);
    }

    @Nonnull
    public ByteString getValue() {
        return this.value;
    }

    @Nonnull
    public KeySpacePath getPath() {
        return path;
    }

    @Nullable
    public Tuple getRemainder() {
        return remainder;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DataInKeySpacePath that = (DataInKeySpacePath)o;
        return Objects.equals(path, that.path) && Objects.equals(remainder, that.remainder) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, remainder, value);
    }

    @Override
    public String toString() {
        return path + "+" + remainder + "->" + value;
    }
}
