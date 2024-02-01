/*
 * ValueWithChild.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A scalar value type that has children.
 */
@API(API.Status.EXPERIMENTAL)
public interface ValueWithChild extends Value {

    /**
     * Method to retrieve the only child value.
     * @return this child {@link Value}
     */
    @Nonnull
    Value getChild();

    @Nonnull
    @Override
    default ValueWithChild withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        return withNewChild(Iterables.getOnlyElement(newChildren));
    }

    @Nonnull
    ValueWithChild withNewChild(@Nonnull Value rebasedChild);
}
