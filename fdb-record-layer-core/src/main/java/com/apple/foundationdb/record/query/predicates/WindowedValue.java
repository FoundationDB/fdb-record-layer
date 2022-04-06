/*
 * WindowedValue.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class WindowedValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Windowed-Value");

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final List<Value> argumentValues;

    protected WindowedValue(@Nonnull Iterable<? extends Value> partitioningValues,
                            @Nonnull Iterable<? extends Value> argumentValues) {
        Preconditions.checkArgument(!Iterables.isEmpty(argumentValues));
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.argumentValues = ImmutableList.copyOf(argumentValues);
    }

    @Nonnull
    public List<Value> getPartitioningValues() {
        return partitioningValues;
    }

    @Nonnull
    public List<Value> getArgumentValues() {
        return argumentValues;
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.<Value>builder().addAll(partitioningValues).addAll(argumentValues).build();
    }

    @Nonnull
    protected Pair<List<Value>, List<Value>> splitNewChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        // We need to split the partitioning and the argument columns by position.
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();

        final var newPartitioningValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, partitioningValues.size()));
        final var newArgumentValues =
                ImmutableList.<Value>copyOf(newChildrenIterator);
        return Pair.of(newPartitioningValues, newArgumentValues);
    }

    @Nonnull
    public abstract String getName();

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, getName(), partitioningValues, argumentValues);
    }

    /**
     * Base implementation of {@link #planHash}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash} so that they are
     * guided to add their own class modifier (See {@link com.apple.foundationdb.record.ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param hashKind the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashKind hashKind, ObjectPlanHash baseHash, Object... hashables) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, baseHash, getName(), partitioningValues, argumentValues, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return getName() + "(" +
               argumentValues.stream().map(a -> a.explain(formatter)).collect(Collectors.joining(", ")) + " PARTITION BY [" +
               partitioningValues.stream().map(a -> a.explain(formatter)).collect(Collectors.joining(", ")) +
               "])";
    }

    @Override
    public String toString() {
        return getName() + "(" +
               argumentValues.stream().map(Value::toString).collect(Collectors.joining(", ")) + " PARTITION BY [" +
               partitioningValues.stream().map(Value::toString).collect(Collectors.joining(", ")) +
               "])";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!Value.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        final var otherWindowedValue = (WindowedValue)other;
        return getName().equals(otherWindowedValue.getName());
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }
}
