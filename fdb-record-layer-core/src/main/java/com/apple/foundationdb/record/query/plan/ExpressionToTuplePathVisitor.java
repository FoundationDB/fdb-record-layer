/*
 * ExpressionToTuplePathVisitor.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.AvailableFields.FieldData;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.primitives.ImmutableIntArray;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;

/**
 * Visitor to find mappings of {@link KeyExpression}s to tuple access paths that are encoded as {@link FieldData}.
 * <br>
 * This visitor is exclusively used by {@link AvailableFields} to compute the mappings from a key expression
 * which is expressed based on a record to an ordinal path (a dewey id) that encodes the access path to the same data
 * but held in e.g. a primary key of an index.
 * <br>
 * Example: A record {@code (a: 10, (b: 30, c: 40), d: 50, x: (y: (e: 60, f: 70), g: 80), h: 90)} can be encoded as
 * a tuple {@code (10, (30, 40), 50, ((60, 70), 80), 90)} which can be addressed by ordinals. For instance,
 * {@code x.y.f can be mapped to the path {@code 3.0.1}}.
 */
@SuppressWarnings("UnstableApiUsage")
public class ExpressionToTuplePathVisitor implements KeyExpressionVisitor<ExpressionToTuplePathVisitor.State, ExpressionToTuplePathVisitor.Result> {
    @Nonnull
    private final KeyExpression keyExpression;

    @Nonnull
    private final TupleSource tupleSource;

    private final int startOrdinal;

    @Nonnull
    private final BitSet skipSet;

    /**
     * We maintain a conceptual stack for the states. States pass information down the visited structure while
     * {@link GraphExpansion}s pass the resulting query graph expansion back up. We use a {@link Deque} here
     * (via {@link ArrayDeque}) as {@link java.util.Stack} is synchronized upon access.
     */
    private final Deque<State> states;

    public ExpressionToTuplePathVisitor(@Nonnull final KeyExpression keyExpression,
                                        @Nonnull final TupleSource tupleSource,
                                        final int startOrdinal,
                                        @Nonnull final BitSet skipSet) {
        this.keyExpression = keyExpression;
        this.tupleSource = tupleSource;
        this.startOrdinal = startOrdinal;
        this.skipSet = skipSet;
        this.states = new ArrayDeque<>();
    }

    @Override
    public State getCurrentState() {
        return states.peek();
    }

    public ExpressionToTuplePathVisitor push(final State newState) {
        states.push(newState);
        return this;
    }

    /**
     * Method to pop and return the top of the states stack.
     * @return the currentState
     */
    public State pop() {
        return states.pop();
    }

    /**
     * Functional pop to facilitate a more fluent way of interacting with the states.
     * @param <T> a type parameter
     * @param t some value of type {@code T}
     * @return {@code t}
     */
    public <T> T pop(final T t) {
        pop();
        return t;
    }

    /**
     * Specific implementation of the fall-back visitation method.
     * @param keyExpression key expression to visit
     * @return does not return a result but throws an exception of type {@link UnsupportedOperationException}
     */
    @Nonnull
    @Override
    public final Result visitExpression(@Nonnull final KeyExpression keyExpression) {
        return visitDefault(keyExpression);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final EmptyKeyExpression emptyKeyExpression) {
        return visitDefault(emptyKeyExpression);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull FieldKeyExpression fieldKeyExpression) {
        final KeyExpression.FanType fanType = fieldKeyExpression.getFanType();
        if (fanType != KeyExpression.FanType.None) {
            throw new RecordCoreException("cannot handle this expression");
        }

        return visitDefault(fieldKeyExpression);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final KeyExpressionWithValue keyExpressionWithValue) {
        return visitDefault(keyExpressionWithValue);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final FunctionKeyExpression functionKeyExpression) {
        return visitDefault(functionKeyExpression);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final KeyWithValueExpression keyWithValueExpression) {
        throw new RecordCoreException("cannot handle this expression");
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final NestingKeyExpression nestingKeyExpression) {
        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final KeyExpression.FanType fanType = parent.getFanType();
        if (fanType != KeyExpression.FanType.None) {
            throw new RecordCoreException("cannot handle this expression");
        }

        final var childResult = pop(nestingKeyExpression.getChild().expand(push(getCurrentState())));
        final var childMap = childResult.getExpressionToFieldDataMap();
        final var childNumSkippedMappings = childResult.getNumSkippedMappings();
        final var resultBuilder = ImmutableListMultimap.<KeyExpression, FieldData>builder();
        for (final var entry : childMap.entries()) {
            resultBuilder.put(new NestingKeyExpression(parent, entry.getKey()), entry.getValue());
        }
        return new Result(resultBuilder.build(), childNumSkippedMappings);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final ThenKeyExpression thenKeyExpression) {
        final var state = getCurrentState();
        final var children = thenKeyExpression.getChildren();
        final var resultBuilder = ImmutableListMultimap.<KeyExpression, FieldData>builder();
        int ordinal = getCurrentState().getOrdinalWithParent().getOrdinal();
        int numSkippedMappings = 0;
        for (final var child : children) {
            final var childResult = pop(child.expand(push(state.withOrdinal(ordinal, numSkippedMappings))));
            final var childMap = childResult.getExpressionToFieldDataMap();
            resultBuilder.putAll(childMap);
            final var childNumSkippedMappings = childResult.getNumSkippedMappings();
            numSkippedMappings += childNumSkippedMappings;
            ordinal += childMap.size();
        }

        return new Result(resultBuilder.build(), numSkippedMappings);
    }

    @Nonnull
    @Override
    public Result visitExpression(@Nonnull final ListKeyExpression listKeyExpression) {
        final var state = getCurrentState();
        final var children = listKeyExpression.getChildren();
        final var resultMapBuilder = ImmutableListMultimap.<KeyExpression, FieldData>builder();
        int ordinal = state.getOrdinalWithParent().getOrdinal();
        for (final var child : children) {
            final var childResult = pop(child.expand(push(state.withNestedOrdinal(ordinal))));
            childResult.getExpressionToFieldDataMap()
                    .entries()
                    .forEach(entry -> {
                        final var keyExpression = entry.getKey();
                        final var fieldData = entry.getValue();
                        resultMapBuilder.put(keyExpression, FieldData.ofUnconditional(fieldData.getSource(), fieldData.getOrdinalPath()));
                    });
            ordinal ++;
        }

        return new Result(resultMapBuilder.build(), 0);
    }

    @Nonnull
    public final Result visitDefault(@Nonnull final KeyExpression keyExpression) {
        final var currentState = getCurrentState();
        final var path = currentState.getOrdinalWithParent().toPath();

        if (path.length() == 1) {
            // We need to compute the index of the corresponding bit in the skip set (which represents parts of the
            // primary key that are used in the declared expression of the index). This can only ever happen at top level.
            // We need to normalize for the startOrdinal which is just offset to shift everything by a fixed amount.
            // In addition to that we also need back in the number of all the previously skipped mappings (as the
            // maintained current ordinal ignores all the skipped mappings, however, the bit set does not).
            final var adjustedIndex = currentState.getOrdinalWithParent().getOrdinal() - startOrdinal + currentState.getNumSkippedMappings();
            if (skipSet.get(adjustedIndex)) {
                return new Result(ImmutableListMultimap.of(), 1);
            }
        }
        return new Result(ImmutableListMultimap.of(keyExpression, FieldData.ofUnconditional(tupleSource, path)), 0);
    }

    @Nonnull
    public ListMultimap<KeyExpression, FieldData> compute() {
        return pop(keyExpression.expand(push(State.ofStartingOrdinal(startOrdinal)))).getExpressionToFieldDataMap();
    }

    /**
     * Helper class to represent a dewey id while traversing the expression.
     */
    private static class OrdinalWithParent {
        @Nullable
        private final OrdinalWithParent parent;
        private final int ordinal;

        public OrdinalWithParent(@Nullable final OrdinalWithParent parent, final int ordinal) {
            this.parent = parent;
            this.ordinal = ordinal;
        }

        @Nullable
        public OrdinalWithParent getParent() {
            return parent;
        }

        public int getOrdinal() {
            return ordinal;
        }

        @Nonnull
        public ImmutableIntArray toPath() {
            final var builder = ImmutableIntArray.builder();
            buildPath(builder);
            return builder.build();
        }

        private void buildPath(@Nonnull final ImmutableIntArray.Builder builder) {
            if (parent != null) {
                parent.buildPath(builder);
            }
            builder.add(ordinal);
        }
    }

    /**
     * State class.
     */
    public static class State implements KeyExpressionVisitor.State {
        @Nonnull
        private final OrdinalWithParent ordinalWithParent;

        private final int numSkippedMappings;

        private State(@Nonnull final OrdinalWithParent ordinalWithParent, final int numSkippedMappings) {
            this.ordinalWithParent = ordinalWithParent;
            this.numSkippedMappings = numSkippedMappings;
        }

        @Nonnull
        public OrdinalWithParent getOrdinalWithParent() {
            return ordinalWithParent;
        }

        public int getNumSkippedMappings() {
            return numSkippedMappings;
        }

        @Nonnull
        public State withOrdinal(final int ordinal, final int numSkippedMappings) {
            return of(new OrdinalWithParent(ordinalWithParent.getParent(), ordinal), numSkippedMappings);
        }

        @Nonnull
        public State withNestedOrdinal(final int ordinal) {
            return of(new OrdinalWithParent(new OrdinalWithParent(ordinalWithParent.getParent(), ordinal), 0), 0);
        }

        @Nonnull
        public static State of(@Nonnull final OrdinalWithParent ordinalWithParent, final int numSkippedMappings) {
            return new State(ordinalWithParent, numSkippedMappings);
        }

        @Nonnull
        public static State ofStartingOrdinal(final int startingOrdinal) {
            return new State(new OrdinalWithParent(null, startingOrdinal), 0);
        }
    }

    /**
     * Result class.
     */
    public static class Result {
        @Nonnull
        private final ListMultimap<KeyExpression, FieldData> expressionToTuplePathMap;

        private final int numSkippedMappings;

        public Result(@Nonnull final ListMultimap<KeyExpression, FieldData> expressionToTuplePathMap, final int numSkippedMappings) {
            this.expressionToTuplePathMap = expressionToTuplePathMap;
            this.numSkippedMappings = numSkippedMappings;
        }

        @Nonnull
        public ListMultimap<KeyExpression, FieldData> getExpressionToFieldDataMap() {
            return expressionToTuplePathMap;
        }

        public int getNumSkippedMappings() {
            return numSkippedMappings;
        }
    }
}
