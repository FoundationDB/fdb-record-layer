/*
 * ResolverKeyValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.Objects;


/**
 * Encapsulates both the key and its resolved value from a {@link LocatableResolver}.
 */
@API(API.Status.MAINTAINED)
public class ResolverKeyValue {
    @Nonnull
    private final String key;
    @Nonnull
    private final ResolverResult value;

    public ResolverKeyValue(@Nonnull final String key, @Nonnull final ResolverResult value) {
        this.key = key;
        this.value = value;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    @Nonnull
    public ResolverResult getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ResolverKeyValue that = (ResolverKeyValue)o;
        return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "Key: " + key + ", " + value;
    }
}
