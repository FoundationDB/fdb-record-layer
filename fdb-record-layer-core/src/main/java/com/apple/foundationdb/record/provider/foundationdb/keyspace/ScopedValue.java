/*
 * ScopedValue.java
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

import java.util.Objects;

/**
 * Object to key the forward and reverse directory caches by, two entries should be treated as identical if they
 * attempt to resolve the same string with directory layers that are located at the same path.
 * @param <T> the type of the scoped value
 */
@API(API.Status.INTERNAL)
public class ScopedValue<T> {
    private final T data;
    private final LocatableResolver scope;

    ScopedValue(T data, LocatableResolver scope) {
        this.data = data;
        this.scope = scope;
    }

    public T getData() {
        return data;
    }

    public LocatableResolver getScope() {
        return scope;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof ScopedValue) {
            ScopedValue<?> that = (ScopedValue) obj;
            return Objects.equals(this.data, that.data) &&
                    Objects.equals(this.scope, that.scope);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data, this.scope);
    }

    @Override
    public String toString() {
        return "[scope: " + scope.toString() + ", data: " + data.toString() + "]";
    }
}

