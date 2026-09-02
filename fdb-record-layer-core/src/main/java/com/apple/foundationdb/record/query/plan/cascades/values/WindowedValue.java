/*
 * WindowedValue.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PWindowedValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.jspecify.annotations.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class WindowedValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Windowed-Value");

    private final List<Value> partitioningValues;

    private final List<Value> argumentValues;

    protected WindowedValue(final PlanSerializationContext serializationContext,
                            final PWindowedValue windowedValueProto) {
        this(windowedValueProto.getPartitioningValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                windowedValueProto.getArgumentValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()));
    }

    protected WindowedValue(Iterable<? extends Value> partitioningValues,
                            Iterable<? extends Value> argumentValues) {
        Preconditions.checkArgument(!Iterables.isEmpty(argumentValues));
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.argumentValues = ImmutableList.copyOf(argumentValues);
    }

    public List<Value> getPartitioningValues() {
        return partitioningValues;
    }

    public List<Value> getArgumentValues() {
        return argumentValues;
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.<Value>builder().addAll(partitioningValues).addAll(argumentValues).build();
    }

    protected NonnullPair<List<Value>, List<Value>> splitNewChildren(final Iterable<? extends Value> newChildren) {
        // We need to split the partitioning and the argument columns by position.
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();

        final var newPartitioningValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, partitioningValues.size()));
        final var newArgumentValues =
                ImmutableList.<Value>copyOf(newChildrenIterator);
        return NonnullPair.of(newPartitioningValues, newArgumentValues);
    }

    public abstract String getName();

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getName());
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)} so
     * that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any); individual elements may be
     *        {@code null} (e.g. an unset optional parameter) since {@link PlanHashable#objectsPlanHash} treats a
     *        {@code null} element as contributing a hash of {@code 0}.
     * @return the plan hash value calculated
     */
    protected int basePlanHash(final PlanHashMode mode, ObjectPlanHash baseHash, @Nullable Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, baseHash, getName(), partitioningValues, argumentValues, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    @SuppressWarnings("PMD.ForLoopCanBeForeach")
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        int i = 0;
        final var partitioningBuilder = ImmutableList.<ExplainTokens>builder();
        final var iterator = explainSuppliers.iterator();
        for (; i < partitioningValues.size(); i ++) {
            partitioningBuilder.add(iterator.next().get().getExplainTokens());
        }
        final var argumentsBuilder = ImmutableList.<ExplainTokens>builder();
        while (iterator.hasNext()) {
            argumentsBuilder.add(iterator.next().get().getExplainTokens());
        }

        final var allArgumentsExplainTokens =
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        argumentsBuilder.build());
        final var partitioning = partitioningBuilder.build();
        if (!partitioning.isEmpty()) {
            allArgumentsExplainTokens.addWhitespace().addKeyword("PARTITION").addWhitespace().addKeyword("BY")
                    .addWhitespace().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), partitioning);
        }

        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall(getName(), allArgumentsExplainTokens));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> getName().equals(((WindowedValue)other).getName()));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    PWindowedValue toWindowedValueProto(final PlanSerializationContext serializationContext) {
        final PWindowedValue.Builder builder = PWindowedValue.newBuilder();
        for (final Value partitioningValue : partitioningValues) {
            builder.addPartitioningValues(partitioningValue.toValueProto(serializationContext));
        }
        for (final Value argumentValue : argumentValues) {
            builder.addArgumentValues(argumentValue.toValueProto(serializationContext));
        }
        return builder.build();
    }
}
