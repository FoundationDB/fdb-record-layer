/*
 * WindowValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;

/**
 * Base class for the window values that appear in a
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression}: a window function together
 * with the frame it sees.
 *
 * <p>Unlike {@link CompileTimeWindowValue}, which these values are derived from, a window value carries neither the
 * {@code PARTITION BY} nor the {@code ORDER BY} columns. Those describe how the input has to be arranged for the
 * function to be computable rather than what the function computes, so they belong to the owning window expression,
 * which is what the planner reasons about when it decides how to satisfy them.
 *
 * <p>The arguments and the frame are common to every window function and live here. Each function gets its own subclass
 * for what genuinely differs: its name, its result type, whether an index can supply it, and -- once window functions
 * are computed rather than matched -- the running state it needs. A call site is discriminated by a
 * {@link CompileTimeWindowValue.WindowFunction} instead, since a call site is only ever a carrier.
 *
 * <p>A window value is a {@link Value.IndexOnlyValue} for now, meaning it can only be produced by an index scan that
 * already materializes it. Computing it in a streaming window operator instead is what would lift that restriction.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class WindowValue extends AbstractValue implements Value.IndexOnlyValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Window-Value");

    @Nonnull
    private final List<Value> argumentValues;

    @Nonnull
    private final WindowFrameSpecification windowFrameSpecification;

    protected WindowValue(@Nonnull final Iterable<? extends Value> argumentValues,
                          @Nonnull final WindowFrameSpecification windowFrameSpecification) {
        this.argumentValues = ImmutableList.copyOf(argumentValues);
        this.windowFrameSpecification = windowFrameSpecification;
    }

    protected WindowValue(@Nonnull final PlanSerializationContext serializationContext,
                          @Nonnull final PWindowValue windowValueProto) {
        this(windowValueProto.getArgumentValuesList()
                        .stream()
                        .map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()),
                windowValueProto.hasFrameSpecification()
                        ? WindowFrameSpecification.fromProto(serializationContext, windowValueProto.getFrameSpecification())
                        : WindowFrameSpecification.defaultSpecification());
    }

    @Nonnull
    public List<Value> getArgumentValues() {
        return argumentValues;
    }

    @Nonnull
    public WindowFrameSpecification getWindowFrameSpecification() {
        return windowFrameSpecification;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return argumentValues;
    }

    /**
     * Returns the name of the window function this value computes, as it is spelled in SQL.
     *
     * @return the window function's name
     */
    @Nonnull
    public abstract String getName();

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var argumentsBuilder = ImmutableList.<ExplainTokens>builder();
        for (final var explainSupplier : explainSuppliers) {
            argumentsBuilder.add(explainSupplier.get().getExplainTokens());
        }
        final var argumentsExplainTokens =
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        argumentsBuilder.build());
        final var resultTokens = new ExplainTokens().addFunctionCall(getName(), argumentsExplainTokens);

        // the OVER group is spelled out even when it is empty, the same way the compile-time value spells it: a window
        // function without one is not valid SQL and would be indistinguishable from a scalar call
        resultTokens.addWhitespace().addKeyword("OVER").addWhitespace()
                .addOpeningParen().addOptionalWhitespace();
        if (!windowFrameSpecification.isDefault()) {
            resultTokens.addNested(windowFrameSpecification.explain());
        }
        resultTokens.addOptionalWhitespace().addClosingParen();

        return ExplainTokensWithPrecedence.of(resultTokens);
    }

    @Nonnull
    protected PWindowValue toWindowValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PWindowValue.newBuilder()
                .setFrameSpecification(windowFrameSpecification.toProto(serializationContext));
        for (final Value argumentValue : argumentValues) {
            builder.addArgumentValues(argumentValue.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getName(),
                windowFrameSpecification);
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}. Concrete window values implement
     * {@link #planHash(PlanHashMode)} in terms of this method so that each of them contributes its own class modifier
     * (see {@link ObjectPlanHash}); the arguments and the frame are common to all of them and are hashed here.
     *
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash
     * @param hashables the rest of the subclass' hashable parameters, if any
     *
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashMode mode, @Nonnull final ObjectPlanHash baseHash,
                              final Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                // the arguments are hashed as well so that two calls of the same function over different arguments do
                // not collide
                return PlanHashable.objectsPlanHash(mode, baseHash, getName(), argumentValues,
                        windowFrameSpecification, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final var otherWindowValue = (WindowValue)other;
                    return getName().equals(otherWindowValue.getName()) &&
                            windowFrameSpecification.equals(otherWindowValue.windowFrameSpecification);
                });
    }
}
