/*
 * ValueIndexLikeExpansionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.temp.ValueIndexLikeExpansionVisitor.VisitorState;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Expansion visitor that implements the shared logic between primary scan data access and value index access.
 */
@SuppressWarnings("java:S5993")
public abstract class ValueIndexLikeExpansionVisitor implements ExpansionVisitor<VisitorState> {

    /**
     * We maintain a conceptual stack for the states. States pass information down the visited structure while
     * {@link GraphExpansion}s pass the resulting query graph expansion back up. We use a {@link Deque} here
     * (via {@link ArrayDeque}) as {@link java.util.Stack} is synchronized upon access.
     */
    private final Deque<VisitorState> states;

    public ValueIndexLikeExpansionVisitor() {
        this.states = new ArrayDeque<>();
    }

    @Override
    public VisitorState getCurrentState() {
        return states.peek();
    }

    public ValueIndexLikeExpansionVisitor push(final VisitorState newState) {
        states.push(newState);
        return this;
    }

    /**
     * Method to pop and return the top of the states stack.
     * @return the currentState
     */
    public VisitorState pop() {
        return states.pop();
    }

    /**
     * Functional pop to facilitate a more fluent way of interacting with the states.
     * @param <T> a type parameter
     * @param t some value of type {@code T}
     * @return {@code t}
     */
    public <T> T pop(final T t) {
        states.pop();
        return t;
    }

    /**
     * Specific implementation of the fall-back visitation method. Sub classes of this class do not tolerate visits
     * from unknown sub classes of {@link KeyExpression}. Implementors of new sub classes of {@link KeyExpression}
     * should also add a new visitation method in {@link KeyExpressionVisitor}.
     * @param keyExpression key expression to visit
     * @return does not return a result but throws an exception of type {@link UnsupportedOperationException}
     */
    @Nonnull
    @Override
    public final GraphExpansion visitExpression(@Nonnull final KeyExpression keyExpression) {
        throw new UnsupportedOperationException("visitor method for this key expression is not implemented");
    }


    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final EmptyKeyExpression emptyKeyExpression) {
        return GraphExpansion.empty();
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull FieldKeyExpression fieldKeyExpression) {
        final String fieldName = fieldKeyExpression.getFieldName();
        final KeyExpression.FanType fanType = fieldKeyExpression.getFanType();
        final VisitorState state = getCurrentState();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final CorrelationIdentifier baseAlias = state.getBaseAlias();
        final List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(fieldName)
                .build();
        final Value value;
        final Placeholder predicate;
        switch (fanType) {
            case FanOut:
                // explode this field and prefixes of this field
                final Quantifier childBase = fieldKeyExpression.explodeField(baseAlias, fieldNamePrefix);
                value = new QuantifiedObjectValue(childBase.getAlias());
                final GraphExpansion childExpansion;
                if (state.isKey()) {
                    predicate = value.asPlaceholder(newParameterAlias());
                    childExpansion = GraphExpansion.ofPlaceholder(value, predicate);
                } else {
                    childExpansion = GraphExpansion.ofResultValue(value);
                }
                final SelectExpression selectExpression =
                        childExpansion
                                .buildSelectWithBase(childBase);
                final Quantifier childQuantifier = Quantifier.forEach(GroupExpressionRef.of(selectExpression));
                return childExpansion
                        .seal()
                        .derivedWithQuantifier(childQuantifier);
            case None:
                value = new FieldValue(baseAlias, fieldNames);
                if (state.isKey()) {
                    predicate = value.asPlaceholder(newParameterAlias());
                    return GraphExpansion.ofPlaceholder(value, predicate);
                }
                return GraphExpansion.ofResultValue(value);
            case Concatenate: // TODO collect/concatenate function
            default:
        }
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final KeyExpressionWithValue keyExpressionWithValue) {
        final VisitorState state = getCurrentState();
        final Value value = keyExpressionWithValue.toValue(state.getBaseAlias(), state.getFieldNamePrefix());
        if (state.isKey()) {
            final Placeholder predicate =
                    value.asPlaceholder(newParameterAlias());
            return GraphExpansion.ofPlaceholder(value, predicate);
        }
        return GraphExpansion.ofResultValue(value);
    }
    
    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final KeyWithValueExpression keyWithValueExpression) {
        // Push a new state to indicate that we have to watch out for a split point between keys and values.
        return pop(keyWithValueExpression
                .getInnerKey()
                .expand(push(getCurrentState()
                        .withSplitPointForValues(keyWithValueExpression.getSplitPoint()))));
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final NestingKeyExpression nestingKeyExpression) {
        final VisitorState state = getCurrentState();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final CorrelationIdentifier baseAlias = state.getBaseAlias();

        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final KeyExpression child = nestingKeyExpression.getChild();
        switch (parent.getFanType()) {
            case None:
                List<String> newPrefix = ImmutableList.<String>builder()
                        .addAll(fieldNamePrefix)
                        .add(parent.getFieldName())
                        .build();
                return pop(child.expand(push(state.withFieldNamePrefix(newPrefix))));
            case FanOut:
                // explode the parent field(s) also depending on the prefix
                final Quantifier childBase = parent.explodeField(baseAlias, fieldNamePrefix);
                // expand the children of the key expression and then unify them into an expansion of this expression
                final GraphExpansion.Sealed childExpandedPredicates =
                        pop(child.expand(push(state.withBaseAlias(childBase.getAlias()).withFieldNamePrefix(ImmutableList.of())))).seal();
                final SelectExpression selectExpression =
                        childExpandedPredicates
                                .buildSelectWithBase(childBase);
                final Quantifier childQuantifier = Quantifier.forEach(GroupExpressionRef.of(selectExpression));
                return childExpandedPredicates.derivedWithQuantifier(childQuantifier);
            case Concatenate:
            default:
                throw new RecordCoreException("unsupported fan type");
        }
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final ThenKeyExpression thenKeyExpression) {
        final ImmutableList.Builder<GraphExpansion> expandedPredicatesBuilder = ImmutableList.builder();
        final VisitorState state = getCurrentState();
        int currentOrdinal = state.getCurrentOrdinal();
        for (KeyExpression child : thenKeyExpression.getChildren()) {
            final GraphExpansion graphExpansion = pop(child.expand(push(state.withCurrentOrdinal(currentOrdinal))));
            currentOrdinal += graphExpansion.getResultValues().size();
            expandedPredicatesBuilder.add(graphExpansion);
        }
        return GraphExpansion.ofOthers(expandedPredicatesBuilder.build());
    }

    /**
     * Creates a new parameter alias.
     * @return unique alias based on a UUID if there is no installed
     *         {@link com.apple.foundationdb.record.query.plan.temp.debug.Debugger},
     *         a unique alias based on an increasing number that is human-readable otherwise.
     */
    private static CorrelationIdentifier newParameterAlias() {
        return CorrelationIdentifier.uniqueID(Placeholder.class);
    }

    /**
     * Class that holds the state necessary to expand both primary data access as well as value indexes.
     * This is meant to be a case class. State is immutable, all mutations cause a new state to be generated.
     */
    public static class VisitorState implements KeyExpressionVisitor.State {
        /**
         * Correlated input to operators using the current state. This alias usually refers to the global record type
         * input or an exploded field (an iteration) defining this state.
         */
        @Nonnull
        private final CorrelationIdentifier baseAlias;

        /**
         * List of field names that form a nesting chain of non-repeated fields.
         */
        @Nonnull
        private final List<String> fieldNamePrefix;

        /**
         * Split point ordinal if the key expression contains both keys and values. Comparing {@link #currentOrdinal}
         * and this value allows to infer it the current part of the key expression is a value or a key
         * ({@link #currentOrdinal} < {@code splitPointForValues})
         */
        private final int splitPointForValues;

        /**
         * Ordinal of the current part that is visited by the visitor.
         */
        private final int currentOrdinal;

        private VisitorState(@Nonnull final CorrelationIdentifier baseAlias,
                             @Nonnull final List<String> fieldNamePrefix,
                             final int splitPointForValues,
                             final int currentOrdinal) {
            this.baseAlias = baseAlias;
            this.fieldNamePrefix = fieldNamePrefix;
            this.splitPointForValues = splitPointForValues;
            this.currentOrdinal = currentOrdinal;
        }

        @Nonnull
        public CorrelationIdentifier getBaseAlias() {
            return baseAlias;
        }

        @Nonnull
        public List<String> getFieldNamePrefix() {
            return fieldNamePrefix;
        }

        public int getSplitPointForValues() {
            return splitPointForValues;
        }

        public int getCurrentOrdinal() {
            return currentOrdinal;
        }

        public boolean isKey() {
            return splitPointForValues < 0 || getCurrentOrdinal() <= splitPointForValues;
        }

        public VisitorState withBaseAlias(@Nonnull final CorrelationIdentifier baseAlias) {
            return new VisitorState(baseAlias,
                    this.fieldNamePrefix,
                    this.splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withFieldNamePrefix(@Nonnull final List<String> fieldNamePrefix) {
            return new VisitorState(this.baseAlias,
                    fieldNamePrefix,
                    this.splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withSplitPointForValues(final int splitPointForValues) {
            return new VisitorState(this.baseAlias,
                    fieldNamePrefix,
                    splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withCurrentOrdinal(final int currentOrdinal) {
            return new VisitorState(this.baseAlias,
                    this.fieldNamePrefix,
                    this.splitPointForValues,
                    currentOrdinal);
        }

        public static VisitorState of(@Nonnull final CorrelationIdentifier baseAlias,
                                      @Nonnull final List<String> fieldNamePrefix,
                                      final int splitPointForValues,
                                      final int currentOrdinal) {
            return new VisitorState(baseAlias,
                    fieldNamePrefix,
                    splitPointForValues,
                    currentOrdinal);
        }
    }
}
