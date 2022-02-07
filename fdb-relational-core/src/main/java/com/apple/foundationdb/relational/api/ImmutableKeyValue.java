/*
 * ImmutableKeyValue.java
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

package com.apple.foundationdb.relational.api;

import java.util.Objects;

import javax.annotation.Nonnull;

public class ImmutableKeyValue implements KeyValue {
    private final NestableTuple key;
    private final NestableTuple value;

    public ImmutableKeyValue(@Nonnull NestableTuple key, @Nonnull NestableTuple value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public NestableTuple key() {
        return key;
    }

    @Override
    public int keyColumnCount() {
        return key.getNumFields();
    }

    @Override
    public NestableTuple value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ImmutableKeyValue that = (ImmutableKeyValue) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
