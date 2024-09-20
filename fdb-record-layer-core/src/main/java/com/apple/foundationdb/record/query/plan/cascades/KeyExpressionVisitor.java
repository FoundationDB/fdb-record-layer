/*
 * KeyExpressionVisitor.java
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

import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;

import javax.annotation.Nonnull;

/**
 * An interface to provide a state-based visitor pattern that can traverse a tree of {@link KeyExpression}s.
 * @param <S> the type of the state object which depends on the specific implementation
 * @param <R> the type of the result object which is returned by all visitation methods
 */
public interface KeyExpressionVisitor<S extends KeyExpressionVisitor.State, R> {

    /**
     * Method to return the (immutable) current state.
     * @return the current state of type {@code S}
     */
    S getCurrentState();

    /**
     * Default method that is called on unknown subclasses of {@link KeyExpression}. That makes it possible to
     * add a {@code visitor.visitExpression(this)} regardless whether the visitor defines an actual specific override
     * for the subclass of {@code this} or not.
     *
     * @param keyExpression key expression to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull KeyExpression keyExpression);

    /**
     * Specific method that is called on {@link EmptyKeyExpression}s.
     *
     * @param emptyKeyExpression {@link EmptyKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull EmptyKeyExpression emptyKeyExpression);

    /**
     * Specific method that is called on {@link FieldKeyExpression}s.
     *
     * @param fieldKeyExpression {@link FieldKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull FieldKeyExpression fieldKeyExpression);

    /**
     * Specific method that is called on {@link KeyExpressionWithValue}s.
     *
     * @param keyExpressionWithValue {@link KeyExpressionWithValue} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull KeyExpressionWithValue keyExpressionWithValue);

    /**
     * Specific method that is called on {@link FunctionKeyExpression}s.
     *
     * @param functionKeyExpression {@link FunctionKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull FunctionKeyExpression functionKeyExpression);

    /**
     * Specific method that is called on {@link KeyWithValueExpression}s.
     *
     * @param keyWithValueExpression {@link KeyWithValueExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull KeyWithValueExpression keyWithValueExpression);

    /**
     * Specific method that is called on {@link NestingKeyExpression}s.
     *
     * @param nestingKeyExpression {@link NestingKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull NestingKeyExpression nestingKeyExpression);

    /**
     * Specific method that is called on {@link ThenKeyExpression}s.
     *
     * @param thenKeyExpression {@link ThenKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull ThenKeyExpression thenKeyExpression);

    /**
     * Specific method that is called on {@link ListKeyExpression}s.
     *
     * @param listKeyExpression {@link ListKeyExpression} to visit
     * @return a new expression of type {@code R}
     */
    @Nonnull
    R visitExpression(@Nonnull ListKeyExpression listKeyExpression);

    /**
     * Tag interface to capture state within this visitor.
     */
    interface State {
        // tag
    }
}
