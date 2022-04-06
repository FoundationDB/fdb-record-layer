/*
 * RankValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.query.plan.temp.Type;

import javax.annotation.Nonnull;

/**
 * A windowed value that computes the RANK of a list of expressions which can optionally be partitioned by expressions
 * defining a window.
 */
@API(API.Status.EXPERIMENTAL)
public class RankValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "RANK";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public RankValue(@Nonnull Iterable<? extends Value> partitioningValues,
                     @Nonnull Iterable<? extends Value> argumentValues) {
        super(partitioningValues, argumentValues);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return basePlanHash(hashKind, BASE_HASH);
    }


    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public RankValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new RankValue(childrenPair.getKey(), childrenPair.getValue());
    }
}
