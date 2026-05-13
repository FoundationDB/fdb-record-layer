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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PTransientWindowValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A transient window value used exclusively during plan generation and rewriting. This value is not intended to be
 * visible to the planner directly; instead, the planner should interact with the corresponding window expression and
 * its respective {@code WindowValue} implementation.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class TransientWindowValue extends AbstractValue implements Value.TransientValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Windowed-Value");

    @Nonnull
    private final List<Value> argumentValues;

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final List<WindowOrderingPart> orderingParts;

    @Nonnull
    private final WindowFrameSpecification windowFrameSpecification;

    protected TransientWindowValue(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final PTransientWindowValue windowedValueProto) {
        this(windowedValueProto.getArgumentValuesList()
                .stream()
                .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                .collect(ImmutableList.toImmutableList()),
                windowedValueProto.getPartitioningValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                windowedValueProto.getOrderingPartsList()
                        .stream()
                        .map(partProto -> WindowOrderingPart.fromProto(serializationContext, partProto))
                        .collect(ImmutableList.toImmutableList()),
                windowedValueProto.hasFrameSpecification()
                        ? WindowFrameSpecification.fromProto(serializationContext, windowedValueProto.getFrameSpecification())
                        : WindowFrameSpecification.defaultSpecification());
    }

    protected TransientWindowValue(@Nonnull Iterable<? extends Value> argumentValues,
                          @Nonnull Iterable<? extends Value> partitioningValues) {
        this(argumentValues, partitioningValues, ImmutableList.of(), WindowFrameSpecification.defaultSpecification());
    }

    protected TransientWindowValue(@Nonnull Iterable<? extends Value> argumentValues,
                          @Nonnull Iterable<? extends Value> partitioningValues,
                          @Nonnull Iterable<WindowOrderingPart> orderingParts,
                          @Nonnull WindowFrameSpecification windowFrameSpecification) {
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.argumentValues = ImmutableList.copyOf(argumentValues);
        this.orderingParts = ImmutableList.copyOf(orderingParts);
        this.windowFrameSpecification = windowFrameSpecification;
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
    public List<WindowOrderingPart> getOrderingParts() {
        return orderingParts;
    }

    @Nonnull
    public WindowFrameSpecification getWindowFrameSpecification() {
        return windowFrameSpecification;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.<Value>builder()
                .addAll(partitioningValues)
                .addAll(argumentValues)
                .addAll(orderingParts.stream().map(WindowOrderingPart::getValue).iterator())
                .build();
    }

    @Nonnull
    protected NonnullPair<List<Value>, List<Value>> splitNewChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();

        final var newPartitioningValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, partitioningValues.size()));
        final var newArgumentValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, argumentValues.size()));
        // remaining values are the ordering part values — reconstruct ordering parts with updated values
        return NonnullPair.of(newPartitioningValues, newArgumentValues);
    }

    @Nonnull
    protected List<WindowOrderingPart> splitNewOrderingParts(@Nonnull final Iterable<? extends Value> newChildren) {
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();
        Iterators.advance(newChildrenIterator, partitioningValues.size() + argumentValues.size());
        final var builder = ImmutableList.<WindowOrderingPart>builder();
        for (final WindowOrderingPart orderingPart : orderingParts) {
            builder.add(new WindowOrderingPart(newChildrenIterator.next(), orderingPart.getSortOrder()));
        }
        return builder.build();
    }

    @Nonnull
    public abstract String getName();

    @Nonnull
    public abstract TransientWindowValue withOrderingParts(@Nonnull List<WindowOrderingPart> newOrderingParts);

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getName(), windowFrameSpecification);
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)} so
     * that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     *
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     *
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashMode mode, ObjectPlanHash baseHash, Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, baseHash, getName(), partitioningValues, argumentValues, orderingParts, windowFrameSpecification, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public ExplainTokens describe() {
        return new ExplainTokens()
                .addKeyword(getName())
                .addWhitespace()
                .addNested(windowFrameSpecification.explain());
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.ForLoopCanBeForeach")
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        int i = 0;
        final var partitioningBuilder = ImmutableList.<ExplainTokens>builder();
        final var iterator = explainSuppliers.iterator();
        for (; i < partitioningValues.size(); i++) {
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

        if (!orderingParts.isEmpty()) {
            final var orderingTokens = orderingParts.stream()
                    .map(part -> {
                        final var partTokens = new ExplainTokens()
                                .addNested(part.getValue().explain().getExplainTokens());
                        final var sortOrder = part.getSortOrder();
                        if (sortOrder != OrderingPart.RequestedSortOrder.ANY) {
                            partTokens.addWhitespace().addKeyword(explainSortOrder(sortOrder));
                        }
                        return partTokens;
                    })
                    .collect(ImmutableList.toImmutableList());
            allArgumentsExplainTokens.addWhitespace().addKeyword("ORDER").addWhitespace().addKeyword("BY")
                    .addWhitespace().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), orderingTokens);
        }

        explainFrameSpecification(allArgumentsExplainTokens, windowFrameSpecification);

        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall(getName(), allArgumentsExplainTokens));
    }

    private static void explainFrameSpecification(@Nonnull final ExplainTokens tokens,
                                                   @Nonnull final WindowFrameSpecification spec) {
        tokens.addWhitespace().addNested(spec.explain());
    }

    @Nonnull
    private static String explainSortOrder(@Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        return switch (sortOrder) {
            case ASCENDING -> "ASC";
            case DESCENDING -> "DESC";
            case ASCENDING_NULLS_LAST -> "ASC NULLS LAST";
            case DESCENDING_NULLS_FIRST -> "DESC NULLS FIRST";
            default -> "";
        };
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final var otherWindowValue = (TransientWindowValue)other;
                    return getName().equals(otherWindowValue.getName()) &&
                            windowFrameSpecification.equals(otherWindowValue.windowFrameSpecification) &&
                            orderingParts.equals(otherWindowValue.orderingParts);
                });
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }


    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        throw new RecordCoreException("transient value cannot be evaluated; it must be consumed during plan rewriting");
    }

    @Nonnull
    PTransientWindowValue toWindowedValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PTransientWindowValue.Builder builder = PTransientWindowValue.newBuilder();
        for (final Value partitioningValue : partitioningValues) {
            builder.addPartitioningValues(partitioningValue.toValueProto(serializationContext));
        }
        for (final Value argumentValue : argumentValues) {
            builder.addArgumentValues(argumentValue.toValueProto(serializationContext));
        }
        for (final WindowOrderingPart orderingPart : orderingParts) {
            builder.addOrderingParts(orderingPart.toProto(serializationContext));
        }
        builder.setFrameSpecification(windowFrameSpecification.toProto(serializationContext));
        return builder.build();
    }
}
