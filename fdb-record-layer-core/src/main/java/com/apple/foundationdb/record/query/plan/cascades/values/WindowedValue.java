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
import com.apple.foundationdb.record.planprotos.PFrameSpecification;
import com.apple.foundationdb.record.planprotos.PRequestedOrderingPart;
import com.apple.foundationdb.record.planprotos.PWindowedValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class WindowedValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Windowed-Value");

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final List<Value> argumentValues;

    @Nonnull
    private final List<OrderingPart.RequestedOrderingPart> orderingParts;

    @Nonnull
    private final FrameSpecification windowFrameSpecification;

    protected WindowedValue(@Nonnull final PlanSerializationContext serializationContext,
                            @Nonnull final PWindowedValue windowedValueProto) {
        this(windowedValueProto.getPartitioningValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                windowedValueProto.getArgumentValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                windowedValueProto.hasFrameSpecification()
                        ? windowedValueProto.getOrderingPartsList()
                                .stream()
                                .map(partProto -> new OrderingPart.RequestedOrderingPart(
                                        Value.fromValueProto(serializationContext, partProto.getValue()),
                                        sortOrderFromProto(partProto.getSortOrder())))
                                .collect(ImmutableList.toImmutableList())
                        : windowedValueProto.getArgumentValuesList()
                                .stream()
                                .map(valueProto -> new OrderingPart.RequestedOrderingPart(
                                        Value.fromValueProto(serializationContext, valueProto),
                                        RequestedSortOrder.ANY))
                                .collect(ImmutableList.toImmutableList()),
                windowedValueProto.hasFrameSpecification()
                        ? frameSpecificationFromProto(serializationContext, windowedValueProto.getFrameSpecification())
                        : FrameSpecification.defaultSpecification());
    }

    protected WindowedValue(@Nonnull Iterable<? extends Value> partitioningValues,
                            @Nonnull Iterable<? extends Value> argumentValues) {
        this(partitioningValues, argumentValues, ImmutableList.of(), FrameSpecification.defaultSpecification());
    }

    protected WindowedValue(@Nonnull Iterable<? extends Value> partitioningValues,
                            @Nonnull Iterable<? extends Value> argumentValues,
                            @Nonnull Iterable<OrderingPart.RequestedOrderingPart> orderingParts,
                            @Nonnull FrameSpecification windowFrameSpecification) {
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
    public List<OrderingPart.RequestedOrderingPart> getOrderingParts() {
        return orderingParts;
    }

    @Nonnull
    public FrameSpecification getWindowFrameSpecification() {
        return windowFrameSpecification;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.<Value>builder().addAll(partitioningValues).addAll(argumentValues).build();
    }

    @Nonnull
    protected NonnullPair<List<Value>, List<Value>> splitNewChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        // We need to split the partitioning and the argument columns by position.
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();

        final var newPartitioningValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, partitioningValues.size()));
        final var newArgumentValues =
                ImmutableList.<Value>copyOf(newChildrenIterator);
        return NonnullPair.of(newPartitioningValues, newArgumentValues);
    }

    @Nonnull
    public abstract String getName();

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
                return PlanHashable.objectsPlanHash(mode, baseHash, getName(), partitioningValues, argumentValues, windowFrameSpecification, hashables);
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
                .addNested(windowFrameSpecification.describe());
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
                                                   @Nonnull final FrameSpecification spec) {
        tokens.addWhitespace().addNested(spec.describe());
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

    @Nonnull
    private static String explainExclusion(@Nonnull final FrameSpecification.Exclusion exclusion) {
        return switch (exclusion) {
            case CurrentRow -> "CURRENT ROW";
            case Group -> "GROUP";
            case Ties -> "TIES";
            default -> "NO OTHERS";
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
                    final var otherWindowValue = (WindowedValue)other;
                    return getName().equals(otherWindowValue.getName()) &&
                            windowFrameSpecification.equals(otherWindowValue.windowFrameSpecification);
                });
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    PWindowedValue toWindowedValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PWindowedValue.Builder builder = PWindowedValue.newBuilder();
        for (final Value partitioningValue : partitioningValues) {
            builder.addPartitioningValues(partitioningValue.toValueProto(serializationContext));
        }
        for (final Value argumentValue : argumentValues) {
            builder.addArgumentValues(argumentValue.toValueProto(serializationContext));
        }
        for (final OrderingPart.RequestedOrderingPart orderingPart : orderingParts) {
            builder.addOrderingParts(PRequestedOrderingPart.newBuilder()
                    .setValue(orderingPart.getValue().toValueProto(serializationContext))
                    .setSortOrder(sortOrderToProto(orderingPart.getSortOrder()))
                    .build());
        }
        builder.setFrameSpecification(frameSpecificationToProto(serializationContext, windowFrameSpecification));
        return builder.build();
    }

    @Nonnull
    private static PRequestedOrderingPart.PSortOrder sortOrderToProto(@Nonnull final RequestedSortOrder sortOrder) {
        return switch (sortOrder) {
            case ASCENDING -> PRequestedOrderingPart.PSortOrder.ASCENDING;
            case DESCENDING -> PRequestedOrderingPart.PSortOrder.DESCENDING;
            case ASCENDING_NULLS_LAST -> PRequestedOrderingPart.PSortOrder.ASCENDING_NULLS_LAST;
            case DESCENDING_NULLS_FIRST -> PRequestedOrderingPart.PSortOrder.DESCENDING_NULLS_FIRST;
            case ANY -> PRequestedOrderingPart.PSortOrder.ANY;
        };
    }

    @Nonnull
    private static RequestedSortOrder sortOrderFromProto(@Nonnull final PRequestedOrderingPart.PSortOrder sortOrder) {
        return switch (sortOrder) {
            case ASCENDING -> RequestedSortOrder.ASCENDING;
            case DESCENDING -> RequestedSortOrder.DESCENDING;
            case ASCENDING_NULLS_LAST -> RequestedSortOrder.ASCENDING_NULLS_LAST;
            case DESCENDING_NULLS_FIRST -> RequestedSortOrder.DESCENDING_NULLS_FIRST;
            case ANY -> RequestedSortOrder.ANY;
        };
    }

    @Nonnull
    private static PFrameSpecification frameSpecificationToProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                @Nonnull final FrameSpecification spec) {
        return PFrameSpecification.newBuilder()
                .setFrameType(frameTypeToProto(spec.frameType()))
                .setLeft(frameBoundaryToProto(serializationContext, spec.left()))
                .setRight(frameBoundaryToProto(serializationContext, spec.right()))
                .setExclusion(exclusionToProto(spec.exclusion()))
                .build();
    }

    @Nonnull
    private static FrameSpecification frameSpecificationFromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                 @Nonnull final PFrameSpecification proto) {
        return new FrameSpecification(
                frameTypeFromProto(proto.getFrameType()),
                frameBoundaryFromProto(serializationContext, proto.getLeft()),
                frameBoundaryFromProto(serializationContext, proto.getRight()),
                exclusionFromProto(proto.getExclusion()));
    }

    @Nonnull
    private static PFrameSpecification.PFrameType frameTypeToProto(@Nonnull final FrameSpecification.FrameType frameType) {
        return switch (frameType) {
            case Row -> PFrameSpecification.PFrameType.ROW;
            case Range -> PFrameSpecification.PFrameType.RANGE;
            case Groups -> PFrameSpecification.PFrameType.GROUPS;
        };
    }

    @Nonnull
    private static FrameSpecification.FrameType frameTypeFromProto(@Nonnull final PFrameSpecification.PFrameType proto) {
        return switch (proto) {
            case ROW -> FrameSpecification.FrameType.Row;
            case RANGE -> FrameSpecification.FrameType.Range;
            case GROUPS -> FrameSpecification.FrameType.Groups;
        };
    }

    @Nonnull
    private static PFrameSpecification.PFrameBoundary frameBoundaryToProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                          @Nonnull final FrameSpecification.FrameBoundary boundary) {
        if (boundary instanceof FrameSpecification.Unbounded) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setUnbounded(true).build();
        } else if (boundary instanceof FrameSpecification.Bounded) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setBoundedLimit(((FrameSpecification.Bounded)boundary).limit().toValueProto(serializationContext)).build();
        } else if (boundary instanceof FrameSpecification.CurrentRow) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setCurrentRow(true).build();
        } else {
            throw new IllegalArgumentException("unknown frame boundary type: " + boundary.getClass());
        }
    }

    @Nonnull
    private static FrameSpecification.FrameBoundary frameBoundaryFromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                           @Nonnull final PFrameSpecification.PFrameBoundary proto) {
        if (proto.hasUnbounded()) {
            return FrameSpecification.Unbounded.INSTANCE;
        } else if (proto.hasBoundedLimit()) {
            return new FrameSpecification.Bounded(Value.fromValueProto(serializationContext, proto.getBoundedLimit()));
        } else if (proto.hasCurrentRow()) {
            return new FrameSpecification.CurrentRow();
        } else {
            throw new IllegalArgumentException("unknown frame boundary proto case");
        }
    }

    @Nonnull
    private static PFrameSpecification.PExclusion exclusionToProto(@Nonnull final FrameSpecification.Exclusion exclusion) {
        return switch (exclusion) {
            case NoOther -> PFrameSpecification.PExclusion.NO_OTHER;
            case CurrentRow -> PFrameSpecification.PExclusion.CURRENT_ROW;
            case Group -> PFrameSpecification.PExclusion.GROUP;
            case Ties -> PFrameSpecification.PExclusion.TIES;
        };
    }

    @Nonnull
    private static FrameSpecification.Exclusion exclusionFromProto(@Nonnull final PFrameSpecification.PExclusion proto) {
        return switch (proto) {
            case NO_OTHER -> FrameSpecification.Exclusion.NoOther;
            case CURRENT_ROW -> FrameSpecification.Exclusion.CurrentRow;
            case GROUP -> FrameSpecification.Exclusion.Group;
            case TIES -> FrameSpecification.Exclusion.Ties;
        };
    }

    public record FrameSpecification(@Nonnull FrameType frameType, @Nonnull FrameBoundary left,
                                     @Nonnull FrameBoundary right, @Nonnull Exclusion exclusion) {


        @Nonnull
        public ExplainTokens describe() {
            final var tokens = new ExplainTokens();
            tokens.addKeyword(frameType.name().toUpperCase());
            tokens.addWhitespace().addKeyword("BETWEEN");
            describeBoundary(tokens, left, true);
            tokens.addWhitespace().addKeyword("AND");
            describeBoundary(tokens, right, false);
            if (exclusion != Exclusion.NoOther) {
                tokens.addWhitespace().addKeyword("EXCLUDE").addWhitespace()
                        .addKeyword(explainExclusion(exclusion));
            }
            return tokens;
        }

        private static void describeBoundary(@Nonnull final ExplainTokens tokens,
                                             @Nonnull final FrameBoundary boundary,
                                             final boolean isLeft) {
            if (boundary instanceof Unbounded) {
                tokens.addWhitespace().addKeyword("UNBOUNDED").addWhitespace()
                        .addKeyword(isLeft ? "PRECEDING" : "FOLLOWING");
            } else if (boundary instanceof Bounded bounded) {
                tokens.addWhitespace().addNested(bounded.limit().explain().getExplainTokens()).addWhitespace()
                        .addKeyword(isLeft ? "PRECEDING" : "FOLLOWING");
            } else if (boundary instanceof CurrentRow) {
                tokens.addWhitespace().addKeyword("CURRENT").addWhitespace().addKeyword("ROW");
            }
        }

        public enum FrameType {
            Row,
            Range,
            Groups
        }

        public sealed interface FrameBoundary permits Unbounded, Bounded, CurrentRow  {
        }

        public enum Unbounded implements FrameBoundary {
            INSTANCE
        }

        public record Bounded(@Nonnull Value limit) implements FrameBoundary {
            public Bounded {
                if (!limit.isConstant()) {
                    throw new IllegalArgumentException("window frame boundary limit must be a constant value");
                }
            }
        }

        public record CurrentRow() implements FrameBoundary {
        }

        public enum Exclusion {
            NoOther,
            CurrentRow,
            Group,
            Ties
        }

        @Nonnull
        public static FrameSpecification defaultSpecification() {
            return new FrameSpecification(FrameType.Row, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.NoOther);
        }
    }
}
