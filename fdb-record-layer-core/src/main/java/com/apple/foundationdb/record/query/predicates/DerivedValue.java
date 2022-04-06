/*
 * DerivedValue.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class DerivedValue implements Value, Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Derived-Value");

    @Nonnull
    private final List<? extends Value> children;

    @Nonnull
    private final Type resultType;

    public DerivedValue(@Nonnull Iterable<? extends Value> values) {
        this(values, Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    public DerivedValue(@Nonnull Iterable<? extends Value> values, @Nonnull Type resultType) {
        this.children = ImmutableList.copyOf(values);
        this.resultType = resultType;
        Preconditions.checkArgument(!children.isEmpty());
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public DerivedValue withChildren(final Iterable<? extends Value> newChildren) {
        return new DerivedValue(newChildren);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, children);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, children);
    }

    @Override
    public String toString() {
        return "derived(" + children.stream()
                .map(Value::toString)
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }
}
