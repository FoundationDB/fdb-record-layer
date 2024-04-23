/*
 * ScalarTranslationVisitor.java
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
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.EmptyValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Visitor that translates a {@link KeyExpression} into a {@link Value}, keeping a state of the currently processed
 * input type while it is processing the {@link KeyExpression}. The main caveat of this visitor is that it is meant to only
 * expand key expressions that are provably scalar.
 */
@SuppressWarnings("java:S5993")
public class ScalarTranslationVisitor implements KeyExpressionVisitor<ScalarTranslationVisitor.ScalarVisitorState, Value> {

    @Nonnull
    private final KeyExpression keyExpression;

    /**
     * We maintain a conceptual stack for the states. States pass information down the visited structure while
     * {@link GraphExpansion}s pass the resulting query graph expansion back up. We use a {@link Deque} here
     * (via {@link ArrayDeque}) as {@link java.util.Stack} is synchronized upon access.
     */
    private final Deque<ScalarVisitorState> states;

    public ScalarTranslationVisitor(@Nonnull final KeyExpression keyExpression) {
        this.keyExpression = keyExpression;
        this.states = new ArrayDeque<>();
    }

    @Override
    public ScalarVisitorState getCurrentState() {
        return states.peek();
    }

    public ScalarTranslationVisitor push(final ScalarVisitorState newState) {
        states.push(newState);
        return this;
    }

    /**
     * Method to pop and return the top of the states stack.
     * @return the currentState
     */
    public ScalarVisitorState pop() {
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
     * Specific implementation of the fall-back visitation method. Sub classes of this class do not tolerate visits
     * from unknown subclasses of {@link KeyExpression}. Implementors of new sub classes of {@link KeyExpression}
     * should also add a new visitation method in {@link KeyExpressionVisitor}.
     * @param keyExpression key expression to visit
     * @return does not return a result but throws an exception of type {@link UnsupportedOperationException}
     */
    @Nonnull
    @Override
    public final Value visitExpression(@Nonnull final KeyExpression keyExpression) {
        throw new UnsupportedOperationException("visitor method for this key expression is not implemented");
    }


    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final EmptyKeyExpression emptyKeyExpression) {
        return EmptyValue.empty();
    }

    @Nonnull
    @Override
    public Value visitExpression(@Nonnull FieldKeyExpression fieldKeyExpression) {
        final KeyExpression.FanType fanType = fieldKeyExpression.getFanType();
        if (fanType != KeyExpression.FanType.None) {
            throw new RecordCoreException("cannot expand fan outs in scalar expansion");
        }

        final ScalarVisitorState state = getCurrentState();
        final String fieldName = fieldKeyExpression.getFieldName();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(fieldName)
                .build();
        return FieldValue.ofFieldNames(QuantifiedObjectValue.of(state.baseAlias, state.inputType), fieldNames);
    }

    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final KeyExpressionWithValue keyExpressionWithValue) {
        final ScalarVisitorState state = getCurrentState();
        return keyExpressionWithValue.toValue(state.getBaseAlias(), state.inputType, state.getFieldNamePrefix());
    }
    
    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final KeyWithValueExpression keyWithValueExpression) {
        throw new RecordCoreException("cannot expand this expression in scalar expansion");
    }

    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final NestingKeyExpression nestingKeyExpression) {
        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final KeyExpression.FanType fanType = parent.getFanType();
        if (fanType != KeyExpression.FanType.None) {
            throw new RecordCoreException("cannot expand fan outs in scalar expansion");
        }

        final ScalarVisitorState state = getCurrentState();
        final List<String> fieldNamePrefix = state.getFieldNamePrefix();
        final KeyExpression child = nestingKeyExpression.getChild();
        final List<String> newPrefix = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(parent.getFieldName())
                .build();
        // TODO resolve type
        return pop(child.expand(push(state.withFieldNamePrefix(newPrefix))));
    }

    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final ThenKeyExpression thenKeyExpression) {
        if (thenKeyExpression.getColumnSize() > 1) {
            throw new RecordCoreException("cannot expand ThenKeyExpression in scalar expansion");
        }

        final ScalarVisitorState state = getCurrentState();
        final KeyExpression child = Iterables.getOnlyElement(thenKeyExpression.getChildren());

        return pop(child.expand(push(state)));
    }

    @Nonnull
    @Override
    public Value visitExpression(@Nonnull final ListKeyExpression listKeyExpression) {
        throw new UnsupportedOperationException("visitor method for this key expression is not implemented");
    }

    @Nonnull
    public Value toResultValue(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
        return pop(keyExpression.expand(push(ScalarVisitorState.of(alias, inputType, ImmutableList.of()))));
    }

    @Nonnull
    public static List<Value> translateKeyExpression(@Nullable KeyExpression keyExpression, @Nonnull Type flowedType) {
        if (keyExpression == null) {
            return ImmutableList.of();
        }

        final var primaryKeyComponents = keyExpression.normalizeKeyForPositions();

        return primaryKeyComponents
                .stream()
                .map(primaryKeyComponent -> new ScalarTranslationVisitor(primaryKeyComponent).toResultValue(Quantifier.current(), flowedType))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * State class.
     */
    public static class ScalarVisitorState implements KeyExpressionVisitor.State {
        /**
         * Correlated input to operators using the current state. This alias usually refers to the global record type
         * input or an exploded field (an iteration) defining this state.
         */
        @Nonnull
        private final CorrelationIdentifier baseAlias;

        /**
         * Input type as the base of expansion.
         */
        @Nonnull
        private final Type inputType;

        /**
         * List of field names that form a nesting chain of non-repeated fields.
         */
        @Nonnull
        private final List<String> fieldNamePrefix;

        private ScalarVisitorState(@Nonnull final CorrelationIdentifier baseAlias,
                                   @Nonnull final Type inputType,
                                   @Nonnull final List<String> fieldNamePrefix) {
            this.baseAlias = baseAlias;
            this.inputType = inputType;
            this.fieldNamePrefix = fieldNamePrefix;
        }

        @Nonnull
        public CorrelationIdentifier getBaseAlias() {
            return baseAlias;
        }

        @Nonnull
        public List<String> getFieldNamePrefix() {
            return fieldNamePrefix;
        }

        public ScalarVisitorState withBaseAlias(@Nonnull final CorrelationIdentifier baseAlias) {
            return of(baseAlias, this.inputType, this.fieldNamePrefix);
        }

        public ScalarVisitorState withFieldNamePrefix(@Nonnull final List<String> fieldNamePrefix) {
            return of(this.baseAlias, this.inputType, fieldNamePrefix);
        }

        public static ScalarVisitorState of(@Nonnull final CorrelationIdentifier baseAlias,
                                            @Nonnull final Type inputType,
                                            @Nonnull final List<String> fieldNamePrefix) {
            return new ScalarVisitorState(baseAlias,
                    inputType,
                    fieldNamePrefix);
        }
    }
}
