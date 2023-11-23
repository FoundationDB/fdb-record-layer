/*
 * BooleanExpressionTrait.java
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

package com.apple.foundationdb.relational.api.fluentsql.expression;

import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.List;

/**
 * This represents a mixin of logical operations.
 */
@Immutable
public interface BooleanExpressionTrait extends Expression<DataType.BooleanType> {

    @Nonnull
    default BooleanExpressionTrait and(@Nonnull final BooleanExpressionTrait right) {
        return new BooleanFunction(Operation.AND, List.of(this, right));
    }

    @Nonnull
    default BooleanExpressionTrait or(@Nonnull final BooleanExpressionTrait right) {
        return new BooleanFunction(Operation.OR, List.of(this, right));
    }

    // todo (yhatem) remove this (post wave3).
    @Nonnull
    default NestedBooleanExpression nested() {
        return new NestedBooleanExpression(this);
    }

    @Nonnull
    default BooleanExpressionTrait not() {
        return new BooleanFunction(Operation.NOT, List.of(this));
    }
}
