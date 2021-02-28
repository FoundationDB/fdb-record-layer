/*
 * LeafValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * A scalar value type that has children.
 */
@API(API.Status.EXPERIMENTAL)
public interface LeafValue extends Value {

    /**
     * Method to retrieve a list of children values.
     * @return a list of children
     */
    @Nonnull
    default Iterable<Value> getChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    default LeafValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        return this;
    }
}
