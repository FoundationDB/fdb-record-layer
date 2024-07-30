/*
 * KeyExpressionExpansionVisitor.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionExpansionVisitor.VisitorState;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.values.EmptyValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Expansion visitor that implements the shared logic between primary scan data access and value index access.
 */
public class KeyExpressionExpansionVisitor implements KeyExpressionVisitor<VisitorState, GraphExpansion> {
    /**
     * We maintain a conceptual stack for the states. States pass information down the visited structure while
     * {@link GraphExpansion}s pass the resulting query graph expansion back up. We use a {@link Deque} here
     * (via {@link ArrayDeque}) as {@link java.util.Stack} is synchronized upon access.
     */
    private final Deque<VisitorState> states;

    public KeyExpressionExpansionVisitor() {
        this.states = new ArrayDeque<>();
    }

    @Override
    public VisitorState getCurrentState() {
        return states.peek();
    }

    public KeyExpressionExpansionVisitor push(final VisitorState newState) {
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
        pop();
        return t;
    }

    /**
     * Specific implementation of the fall-back visitation method. Subclasses of this class do not tolerate visits
     * from unknown subclasses of {@link KeyExpression}. Implementors of new subclasses of {@link KeyExpression}
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
        return GraphExpansion.ofResultColumn(Column.unnamedOf(EmptyValue.empty()));
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull FieldKeyExpression fieldKeyExpression) {
        final String fieldName = fieldKeyExpression.getFieldName();
        final KeyExpression.FanType fanType = fieldKeyExpression.getFanType();
        final VisitorState state = getCurrentState();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final Quantifier.ForEach baseQuantifier = state.getBaseQuantifier();
        final List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(fieldName)
                .build();
        final Value value;
        final Column<?> column;
        switch (fanType) {
            case FanOut:
                // explode this field and prefixes of this field
                final Quantifier.ForEach childBase = fieldKeyExpression.explodeField(baseQuantifier, fieldNamePrefix);
                value = state.registerValue(childBase.getFlowedObjectValue());
                column = Column.unnamedOf(value);
                final GraphExpansion childExpansion;
                if (state.isKey()) {
                    childExpansion = GraphExpansion.ofResultColumnAndPlaceholder(column, value.asPlaceholder(newParameterAlias()));
                } else {
                    childExpansion = GraphExpansion.ofResultColumn(column);
                }
                final SelectExpression selectExpression =
                        childExpansion
                                .withBase(childBase)
                                .buildSelect();
                final Quantifier childQuantifier = Quantifier.forEach(Reference.of(selectExpression));
                final GraphExpansion.Sealed sealedChildExpansion =
                        childExpansion.seal();
                return sealedChildExpansion
                        .builderWithInheritedPlaceholders().pullUpQuantifier(childQuantifier).build();
            case None:
                value = state.registerValue(FieldValue.ofFieldNames(baseQuantifier.getFlowedObjectValue(), fieldNames));
                column = Column.unnamedOf(value);
                if (state.isKey()) {
                    return GraphExpansion.ofResultColumnAndPlaceholder(column, value.asPlaceholder(newParameterAlias()));
                }
                return GraphExpansion.ofResultColumn(column);
            case Concatenate: // TODO collect/concatenate function
            default:
        }
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final KeyExpressionWithValue keyExpressionWithValue) {
        final VisitorState state = getCurrentState();
        final Value value = state.registerValue(keyExpressionWithValue.toValue(state.getBaseQuantifier(), state.getFieldNamePrefix()));
        if (state.isKey()) {
            return GraphExpansion.ofResultColumnAndPlaceholder(Column.unnamedOf(value), value.asPlaceholder(newParameterAlias()));
        }
        return GraphExpansion.ofResultColumn(Column.unnamedOf(value));
    }
    
    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final KeyWithValueExpression keyWithValueExpression) {
        throw new RecordCoreException("expression should have been handled at top level");
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final NestingKeyExpression nestingKeyExpression) {
        final VisitorState state = getCurrentState();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final Quantifier.ForEach baseQuantifier = state.getBaseQuantifier();

        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final KeyExpression child = nestingKeyExpression.getChild();
        switch (parent.getFanType()) {
            case None:
                List<String> newPrefix = ImmutableList.<String>builder()
                        .addAll(fieldNamePrefix)
                        .add(parent.getFieldName())
                        .build();
                if (NullableArrayTypeUtils.isArrayWrapper(nestingKeyExpression)) {
                    final RecordMetaDataProto.KeyExpression childProto = nestingKeyExpression.getChild().toKeyExpression();
                    if (childProto.hasNesting()) {
                        RecordMetaDataProto.Nesting.Builder newNestingBuilder = RecordMetaDataProto.Nesting.newBuilder()
                                .setParent(parent.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT))
                                .setChild(childProto.getNesting().getChild());
                        return visitExpression(new NestingKeyExpression(newNestingBuilder.build()));
                    } else {
                        return visitExpression(new FieldKeyExpression(parent.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT).build()));
                    }
                }
                return pop(child.expand(push(state.withFieldNamePrefix(newPrefix))));
            case FanOut:
                // explode the parent field(s) also depending on the prefix
                final Quantifier.ForEach childBase = parent.explodeField(baseQuantifier, fieldNamePrefix);
                // expand the children of the key expression and then unify them into an expansion of this expression
                final GraphExpansion childExpansion =
                        pop(child.expand(push(state.withBaseQuantifier(childBase).withFieldNamePrefix(ImmutableList.of()))));
                final GraphExpansion baseAndChildExpansion = childExpansion.withBase(childBase);
                final GraphExpansion.Sealed sealedBaseAndChildExpansion = baseAndChildExpansion.seal();
                final SelectExpression selectExpression =
                        sealedBaseAndChildExpansion.buildSelect();
                final Quantifier childQuantifier = Quantifier.forEach(Reference.of(selectExpression));
                return sealedBaseAndChildExpansion
                        .builderWithInheritedPlaceholders()
                        .pullUpQuantifier(childQuantifier)
                        .build();
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
            currentOrdinal += graphExpansion.getResultColumns().size();
            expandedPredicatesBuilder.add(graphExpansion);
        }
        return GraphExpansion.ofOthers(expandedPredicatesBuilder.build());
    }

    @Nonnull
    @Override
    public GraphExpansion visitExpression(@Nonnull final ListKeyExpression listKeyExpression) {
        throw new UnsupportedOperationException("visitor method for this key expression is not implemented");
    }

    /**
     * Creates a new parameter alias.
     * @return unique alias based on a UUID if there is no installed
     *         {@link com.apple.foundationdb.record.query.plan.cascades.debug.Debugger},
     *         a unique alias based on an increasing number that is human-readable otherwise.
     */
    protected static CorrelationIdentifier newParameterAlias() {
        return CorrelationIdentifier.uniqueID(PredicateWithValueAndRanges.class);
    }

    /**
     * Class that holds the state necessary to expand both primary data access as well as value indexes.
     * This is meant to be a case class. State is immutable, all mutations cause a new state to be generated.
     */
    public static class VisitorState implements KeyExpressionVisitor.State {
        /**
         * Correlated input to operators using the current state. This quantifier usually refers to the global record type
         * input or an exploded field (an iteration) defining this state.
         */
        @Nonnull
        private final Quantifier.ForEach baseQuantifier;

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

        /**
         * List of keys as expanded values form in the index.
         */
        @Nonnull
        private final List<Value> keyValues;

        /**
         * List of values as expanded values form in the index.
         */
        @Nonnull
        private final List<Value> valueValues;

        private VisitorState(@Nonnull final List<Value> keyOrdinalMap,
                             @Nonnull final List<Value> valueValues,
                             @Nonnull final Quantifier.ForEach baseQuantifier,
                             @Nonnull final List<String> fieldNamePrefix,
                             final int splitPointForValues,
                             final int currentOrdinal) {
            this.keyValues = keyOrdinalMap;
            this.valueValues = valueValues;
            this.baseQuantifier = baseQuantifier;
            this.fieldNamePrefix = fieldNamePrefix;
            this.splitPointForValues = splitPointForValues;
            this.currentOrdinal = currentOrdinal;
        }

        @Nonnull
        public List<Value> getKeyValues() {
            return keyValues;
        }

        @Nonnull
        public List<Value> getValueValues() {
            return valueValues;
        }

        @Nonnull
        public Quantifier.ForEach getBaseQuantifier() {
            return baseQuantifier;
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
            return splitPointForValues < 0 || getCurrentOrdinal() < splitPointForValues;
        }

        @Nonnull
        public Value registerValue(@Nonnull final Value value) {
            if (isKey()) {
                keyValues.add(value);
            } else {
                valueValues.add(value);
            }
            return value;
        }

        public VisitorState withBaseQuantifier(@Nonnull final Quantifier.ForEach baseQuantifier) {
            return new VisitorState(this.keyValues,
                    this.valueValues,
                    baseQuantifier,
                    this.fieldNamePrefix,
                    this.splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withFieldNamePrefix(@Nonnull final List<String> fieldNamePrefix) {
            return new VisitorState(this.keyValues,
                    this.valueValues,
                    this.baseQuantifier,
                    fieldNamePrefix,
                    this.splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withSplitPointForValues(final int splitPointForValues) {
            return new VisitorState(this.keyValues,
                    this.valueValues,
                    this.baseQuantifier,
                    fieldNamePrefix,
                    splitPointForValues,
                    this.currentOrdinal);
        }

        public VisitorState withCurrentOrdinal(final int currentOrdinal) {
            return new VisitorState(this.keyValues,
                    this.valueValues,
                    this.baseQuantifier,
                    this.fieldNamePrefix,
                    this.splitPointForValues,
                    currentOrdinal);
        }

        public static VisitorState forQueries(@Nonnull final List<Value> valueValues,
                                              @Nonnull final Quantifier.ForEach baseQuantifier,
                                              @Nonnull final List<String> fieldNamePrefix) {
            return new VisitorState(
                    Lists.newArrayList(),
                    valueValues,
                    baseQuantifier,
                    fieldNamePrefix,
                    0,
                    0);
        }

        public static VisitorState of(@Nonnull final List<Value> keyValues,
                                      @Nonnull final List<Value> valueValues,
                                      @Nonnull final Quantifier.ForEach baseQuantifier,
                                      @Nonnull final List<String> fieldNamePrefix,
                                      final int splitPointForValues,
                                      final int currentOrdinal) {
            return new VisitorState(
                    keyValues,
                    valueValues,
                    baseQuantifier,
                    fieldNamePrefix,
                    splitPointForValues,
                    currentOrdinal);
        }
    }
}
