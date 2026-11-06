/*
 * CompileTimeWindowValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PCompileTimeWindowValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A window value that only exists while a plan is being generated and rewritten. It carries the window function's
 * arguments together with the {@code PARTITION BY} columns, the {@code ORDER BY} columns, and the frame specification
 * exactly as they were spelled at the call site, which is what the rewriter needs in order to lift them out into a
 * dedicated window expression.
 *
 * <p>Which window function is being computed is captured by a {@link WindowFunction} rather than by subclassing. This value
 * is a carrier: it holds the call site verbatim and has no per-function behaviour, so a single class parameterized by the
 * function keeps it from being N identical classes. The planner-facing counterpart is the opposite -- there each function
 * gets its own {@link WindowValue} subclass, because that is where the functions actually behave differently. The
 * argument signature is declared by the registered {@link BuiltInFunction} for each function, which is also where a call
 * site is turned into one of these values, and {@link #toWindowValue()} is where the function is mapped to its
 * {@link WindowValue}.
 *
 * <p>This value is not intended to be visible to the planner proper; the planner interacts with the window expression and
 * its respective {@code WindowValue} implementation instead. Being a {@link Value.CompileTimeOnlyValue}, it can be
 * evaluated neither at runtime nor at compile-time: it is a placeholder that must be consumed by plan rewriting, so
 * reaching either {@code eval} method means the rewrite that was supposed to eliminate it did not happen.
 */
@API(API.Status.EXPERIMENTAL)
public final class CompileTimeWindowValue extends AbstractValue implements Value.CompileTimeOnlyValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Compile-Time-Window-Value");

    @Nonnull
    private final WindowFunction windowFunction;

    @Nonnull
    private final List<Value> argumentValues;

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final List<WindowOrderingPart> orderingParts;

    @Nonnull
    private final WindowFrameSpecification windowFrameSpecification;

    private CompileTimeWindowValue(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PCompileTimeWindowValue compileTimeWindowValueProto) {
        this(windowFunctionFromProto(compileTimeWindowValueProto.getWindowFunction()),
                compileTimeWindowValueProto.getArgumentValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                compileTimeWindowValueProto.getPartitioningValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                compileTimeWindowValueProto.getOrderingPartsList()
                        .stream()
                        .map(partProto -> WindowOrderingPart.fromProto(serializationContext, partProto))
                        .collect(ImmutableList.toImmutableList()),
                compileTimeWindowValueProto.hasFrameSpecification()
                        ? WindowFrameSpecification.fromProto(serializationContext, compileTimeWindowValueProto.getFrameSpecification())
                        : WindowFrameSpecification.defaultSpecification());
    }

    public CompileTimeWindowValue(@Nonnull final WindowFunction windowFunction,
                                  @Nonnull final Iterable<? extends Value> argumentValues,
                                  @Nonnull final Iterable<? extends Value> partitioningValues) {
        this(windowFunction, argumentValues, partitioningValues, ImmutableList.of(),
                WindowFrameSpecification.defaultSpecification());
    }

    public CompileTimeWindowValue(@Nonnull final WindowFunction windowFunction,
                                  @Nonnull final Iterable<? extends Value> argumentValues,
                                  @Nonnull final Iterable<? extends Value> partitioningValues,
                                  @Nonnull final Iterable<WindowOrderingPart> orderingParts,
                                  @Nonnull final WindowFrameSpecification windowFrameSpecification) {
        this.windowFunction = windowFunction;
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.argumentValues = ImmutableList.copyOf(argumentValues);
        this.orderingParts = ImmutableList.copyOf(orderingParts);
        this.windowFrameSpecification = windowFrameSpecification;
    }

    @Nonnull
    public WindowFunction getWindowFunction() {
        return windowFunction;
    }

    @Nonnull
    public String getName() {
        return windowFunction.getFunctionName();
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
    public Type getResultType() {
        return windowFunction.getResultType();
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
    @Override
    public CompileTimeWindowValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        final Iterator<? extends Value> newChildrenIterator = newChildren.iterator();
        final var newPartitioningValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, partitioningValues.size()));
        final var newArgumentValues =
                ImmutableList.<Value>copyOf(Iterators.limit(newChildrenIterator, argumentValues.size()));
        final var newOrderingPartsBuilder = ImmutableList.<WindowOrderingPart>builder();
        for (final WindowOrderingPart orderingPart : orderingParts) {
            newOrderingPartsBuilder.add(new WindowOrderingPart(newChildrenIterator.next(), orderingPart.getSortOrder()));
        }
        return new CompileTimeWindowValue(windowFunction, newArgumentValues, newPartitioningValues,
                newOrderingPartsBuilder.build(), windowFrameSpecification);
    }

    /**
     * Returns a copy of this value whose {@code ORDER BY} columns are the given ones.
     *
     * @param newOrderingParts the new ordering parts
     *
     * @return a copy of this value with the given ordering parts
     */
    @Nonnull
    public CompileTimeWindowValue withOrderingParts(@Nonnull final List<WindowOrderingPart> newOrderingParts) {
        return new CompileTimeWindowValue(windowFunction, argumentValues, partitioningValues, newOrderingParts,
                windowFrameSpecification);
    }

    /**
     * Returns the {@link WindowValue} this call site computes, that is this value without the {@code PARTITION BY} and
     * {@code ORDER BY} columns. Those are not dropped: the rewrite that calls this hands them to the enclosing window
     * expression, which is where the planner reasons about how to satisfy them.
     *
     * @return the window value corresponding to this call site
     */
    @Nonnull
    public WindowValue toWindowValue() {
        return switch (windowFunction) {
            case RANK -> new RankWindowValue(argumentValues, windowFrameSpecification);
        };
    }

    @Nullable
    @Override
    public Object evalWithoutStore(@Nonnull final EvaluationContext context) {
        throw new RecordCoreException("compile-time window value cannot be evaluated; it must be consumed during plan rewriting");
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, windowFunction.name(),
                windowFrameSpecification);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return switch (mode.getKind()) {
            case LEGACY, FOR_CONTINUATION ->
                    PlanHashable.objectsPlanHash(mode, BASE_HASH, windowFunction.name(), partitioningValues,
                            argumentValues, orderingParts, windowFrameSpecification);
            default -> throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        };
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
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var iterator = explainSuppliers.iterator();
        final var partitioningBuilder = ImmutableList.<ExplainTokens>builder();
        for (final Value ignored : partitioningValues) {
            partitioningBuilder.add(iterator.next().get().getExplainTokens());
        }
        final var argumentsBuilder = ImmutableList.<ExplainTokens>builder();
        for (final Value ignored : argumentValues) {
            argumentsBuilder.add(iterator.next().get().getExplainTokens());
        }

        final var argumentsExplainTokens =
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        argumentsBuilder.build());
        final var resultTokens = new ExplainTokens().addFunctionCall(getName(), argumentsExplainTokens);

        // the window specification is rendered as its own OVER group, the way it is spelled in SQL, so that the
        // arguments stay distinguishable from the partitioning and the value as a whole stays self-delimiting
        final var specificationSections = ImmutableList.<ExplainTokens>builder();

        final var partitioning = partitioningBuilder.build();
        if (!partitioning.isEmpty()) {
            specificationSections.add(new ExplainTokens()
                    .addKeyword("PARTITION").addWhitespace().addKeyword("BY").addWhitespace()
                    .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), partitioning));
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
            specificationSections.add(new ExplainTokens()
                    .addKeyword("ORDER").addWhitespace().addKeyword("BY").addWhitespace()
                    .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), orderingTokens));
        }

        if (!windowFrameSpecification.isDefault()) {
            specificationSections.add(windowFrameSpecification.explain());
        }

        resultTokens.addWhitespace().addKeyword("OVER").addWhitespace()
                .addOpeningParen().addOptionalWhitespace()
                .addSequence(() -> new ExplainTokens().addWhitespace(), specificationSections.build())
                .addOptionalWhitespace().addClosingParen();

        return ExplainTokensWithPrecedence.of(resultTokens);
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
                    final var otherWindowValue = (CompileTimeWindowValue)other;
                    return windowFunction == otherWindowValue.windowFunction &&
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

    @Nonnull
    @Override
    public PCompileTimeWindowValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PCompileTimeWindowValue.Builder builder = PCompileTimeWindowValue.newBuilder()
                .setWindowFunction(windowFunctionToProto(windowFunction));
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

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCompileTimeWindowValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static CompileTimeWindowValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PCompileTimeWindowValue compileTimeWindowValueProto) {
        return new CompileTimeWindowValue(serializationContext, compileTimeWindowValueProto);
    }

    @Nonnull
    private static PCompileTimeWindowValue.PWindowFunction windowFunctionToProto(@Nonnull final WindowFunction windowFunction) {
        return switch (windowFunction) {
            case RANK -> PCompileTimeWindowValue.PWindowFunction.RANK;
        };
    }

    @Nonnull
    private static WindowFunction windowFunctionFromProto(@Nonnull final PCompileTimeWindowValue.PWindowFunction proto) {
        return switch (proto) {
            case RANK -> WindowFunction.RANK;
        };
    }

    /**
     * The window function a {@link CompileTimeWindowValue} computes. A new window function is added by adding a constant
     * here, a matching constant to {@link PCompileTimeWindowValue.PWindowFunction}, and a registered
     * {@link BuiltInFunction} declaring the arguments it accepts.
     */
    public enum WindowFunction {
        RANK("rank", Type.primitiveType(Type.TypeCode.LONG));

        @Nonnull
        private final String functionName;

        @Nonnull
        private final Type resultType;

        WindowFunction(@Nonnull final String functionName, @Nonnull final Type resultType) {
            this.functionName = functionName;
            this.resultType = resultType;
        }

        @Nonnull
        public String getFunctionName() {
            return functionName;
        }

        @Nonnull
        public Type getResultType() {
            return resultType;
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCompileTimeWindowValue, CompileTimeWindowValue> {
        @Nonnull
        @Override
        public Class<PCompileTimeWindowValue> getProtoMessageClass() {
            return PCompileTimeWindowValue.class;
        }

        @Nonnull
        @Override
        public CompileTimeWindowValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PCompileTimeWindowValue compileTimeWindowValueProto) {
            return CompileTimeWindowValue.fromProto(serializationContext, compileTimeWindowValueProto);
        }
    }

    /**
     * The {@code RANK} window function, and later the {@code RANK} hypothetical-set aggregate.
     *
     * <p>The argument is optional. Standard SQL spells the window function {@code RANK() OVER (… ORDER BY x)} with no
     * argument at all, the operand coming from the ordering; an explicit argument is what distinguishes
     * {@code RANK(v) WITHIN GROUP (ORDER BY x)}, which asks what rank {@code v} would get if it were inserted. Both
     * arities therefore resolve here, and {@link #encapsulateInternal} is where the window spelling is normalized.
     */
    @AutoService(BuiltInFunction.class)
    public static final class RankFn extends BuiltInFunction<CompileTimeWindowValue> {
        public RankFn() {
            super(WindowFunction.RANK.getFunctionName(), ImmutableList.of(), Type.any(),
                    (ignored, callSiteArguments) -> encapsulateInternal(WindowFunction.RANK, callSiteArguments));
        }
    }

    @Nonnull
    private static CompileTimeWindowValue encapsulateInternal(@Nonnull final WindowFunction windowFunction,
                                                              @Nonnull final CallSiteArguments callSiteArguments) {
        final var windowSpecification = Objects.requireNonNull(callSiteArguments.getWindowSpecification());
        final var argumentValues = callSiteArguments.getArgumentsList();
        final var orderingParts = windowSpecification.orderingParts();
        //
        // The column a ranking function ranks by belongs in the ORDER BY of the OVER clause, which is where standard
        // SQL puts it and where the window specification carries it. The argument list stays empty, and an argument
        // here is a different function: rank(v) WITHIN GROUP (ORDER BY x) asks what rank v would get if it were
        // inserted, and has no OVER clause, so it does not reach this method.
        //
        if (!argumentValues.isEmpty()) {
            throw new RecordCoreException("a window function ranks by the ORDER BY of its OVER clause, not by an argument")
                    .addLogInfo(LogMessageKeys.FUNCTION, windowFunction.getFunctionName());
        }
        if (orderingParts.isEmpty()) {
            throw new RecordCoreException("a window function needs an ORDER BY in its OVER clause")
                    .addLogInfo(LogMessageKeys.FUNCTION, windowFunction.getFunctionName());
        }
        return new CompileTimeWindowValue(windowFunction, argumentValues,
                windowSpecification.partitioningValues(), orderingParts,
                windowSpecification.frameSpecification());
    }
}
