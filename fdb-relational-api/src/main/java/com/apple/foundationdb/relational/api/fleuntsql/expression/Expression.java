/*
 * Expression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.fleuntsql.expression;

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.fleuntsql.FluentVisitor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * The super type of all expressions, an expression is typed with a Relational {@link DataType}.
 *
 * @param <T> The type of the expression.
 */
@Immutable
public interface Expression<T extends DataType> {

    @Nullable
    <R, C> R accept(@Nonnull final FluentVisitor<R, C> visitor, @Nonnull final C context);

    DataType getType();
}
