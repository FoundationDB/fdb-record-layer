/*
 * NumericExpressionTrait.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.fluentsql.expression;

import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.List;

/**
 * This is a mixin of numerical operations. Please note that this does not support any kind of SQL-like implicit casting at
 * the moment.
 * @param <N> The type of the function.
 */
@Immutable
public interface NumericExpressionTrait<N extends DataType.NumericType> extends ComparableExpressionTrait<N, Expression<N>> {
    @Nonnull
    default NumericExpressionTrait<N> add(@Nonnull final NumericExpressionTrait<N> right) {
        return new NumericFunction<>(this.getType(), Operation.ADD, List.of(this, right));
    }

    @Nonnull
    default NumericExpressionTrait<N> sub(@Nonnull final NumericExpressionTrait<N> right) {
        return new NumericFunction<>(this.getType(), Operation.SUB, List.of(this, right));
    }

    @Nonnull
    default NumericExpressionTrait<N> div(@Nonnull final NumericExpressionTrait<N> right) {
        return new NumericFunction<>(this.getType(), Operation.DIV, List.of(this, right));
    }

    @Nonnull
    default NumericExpressionTrait<N> mul(@Nonnull final NumericExpressionTrait<N> right) {
        return new NumericFunction<>(this.getType(), Operation.MUL, List.of(this, right));
    }

    @Nonnull
    default NumericExpressionTrait<N> mod(@Nonnull final NumericExpressionTrait<N> right) {
        return new NumericFunction<>(this.getType(), Operation.MOD, List.of(this, right));
    }

    @Override
    N getType();
}
