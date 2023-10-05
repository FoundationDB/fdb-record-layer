/*
 * ExpressionFragment.java
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
import com.apple.foundationdb.relational.api.fleuntsql.expression.details.Mixins;

import javax.annotation.Nonnull;

public interface ExpressionFragment<T extends DataType> extends Expression<T> {

    @Nonnull
    String getFragment();

    @Nonnull
    default Mixins.BooleanExpressionFragment asBoolean() {
        return asBoolean(true);
    }

    @Nonnull
    default Mixins.BooleanExpressionFragment asBoolean(boolean isNullable) {
        if (this instanceof Mixins.BooleanExpressionFragment) {
            return (Mixins.BooleanExpressionFragment) this;
        }
        return Mixins.asBoolean(this, isNullable);
    }

    @Nonnull
    default Mixins.IntExpressionFragment asInt() {
        return asInt(true);
    }

    @Nonnull
    default Mixins.IntExpressionFragment asInt(boolean isNullable) {
        if (this instanceof Mixins.IntExpressionFragment) {
            return (Mixins.IntExpressionFragment) this;
        }
        return Mixins.asInt(this, isNullable);
    }

    @Nonnull
    default Mixins.LongExpressionFragment asLong() {
        return asLong(true);
    }

    @Nonnull
    default Mixins.LongExpressionFragment asLong(boolean isNullable) {
        if (this instanceof Mixins.LongExpressionFragment) {
            return (Mixins.LongExpressionFragment) this;
        }
        return Mixins.asLong(this, isNullable);
    }

    @Nonnull
    default Mixins.FloatExpressionFragment asFloat() {
        return asFloat(true);
    }

    @Nonnull
    default Mixins.FloatExpressionFragment asFloat(boolean isNullable) {
        if (this instanceof Mixins.FloatExpressionFragment) {
            return (Mixins.FloatExpressionFragment) this;
        }
        return Mixins.asFloat(this, isNullable);
    }

    @Nonnull
    default Mixins.DoubleExpressionFragment asDouble() {
        return asDouble(true);
    }

    @Nonnull
    default Mixins.DoubleExpressionFragment asDouble(boolean isNullable) {
        if (this instanceof Mixins.DoubleExpressionFragment) {
            return (Mixins.DoubleExpressionFragment) this;
        }
        return Mixins.asDouble(this, isNullable);
    }

    @Nonnull
    default Mixins.StringExpressionFragment asString() {
        return asString(true);
    }

    @Nonnull
    default Mixins.StringExpressionFragment asString(boolean isNullable) {
        if (this instanceof Mixins.StringExpressionFragment) {
            return (Mixins.StringExpressionFragment) this;
        }
        return Mixins.asString(this, isNullable);
    }
}
