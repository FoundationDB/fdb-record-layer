/*
 * Field.java
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

import com.apple.foundationdb.relational.api.fluentsql.expression.details.Mixins;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Represents a (nested) field in a SQL table.
 *
 * @param <T> The type of the field.
 */
@Immutable
public interface Field<T extends DataType> extends ComparableExpressionTrait<T, Expression<T>> {

    @Nonnull
    Iterable<String> getParts();

    @Nonnull
    Field<?> subField(@Nonnull String part);

    @Nonnull
    String getName();

    @Nonnull
    default Mixins.BooleanField asBoolean() {
        return asBoolean(true);
    }

    default Mixins.BooleanField asBoolean(boolean isNullable) {
        if (this instanceof Mixins.BooleanField) {
            return (Mixins.BooleanField) this;
        }
        return Mixins.asBoolean(this, isNullable);
    }

    @Nonnull
    default Mixins.IntField asInt() {
        return asInt(true);
    }

    @Nonnull
    default Mixins.IntField asInt(boolean isNullable) {
        if (this instanceof Mixins.IntField) {
            return (Mixins.IntField) this;
        }
        return Mixins.asInt(this, isNullable);
    }

    @Nonnull
    default Mixins.LongField asLong() {
        return asLong(true);
    }

    @Nonnull
    default Mixins.LongField asLong(boolean isNullable) {
        if (this instanceof Mixins.LongField) {
            return (Mixins.LongField) this;
        }
        return Mixins.asLong(this, isNullable);
    }

    @Nonnull
    default Mixins.FloatField asFloat() {
        return asFloat(true);
    }

    @Nonnull
    default Mixins.FloatField asFloat(boolean isNullable) {
        if (this instanceof Mixins.FloatField) {
            return (Mixins.FloatField) this;
        }
        return Mixins.asFloat(this, isNullable);
    }

    @Nonnull
    default Mixins.DoubleField asDouble() {
        return asDouble(true);
    }

    @Nonnull
    default Mixins.DoubleField asDouble(boolean isNullable) {
        if (this instanceof Mixins.DoubleField) {
            return (Mixins.DoubleField) this;
        }
        return Mixins.asDouble(this, isNullable);
    }

    @Nonnull
    default Mixins.StringField asString() {
        return asString(true);
    }

    @Nonnull
    default Mixins.StringField asString(boolean isNullable) {
        if (this instanceof Mixins.StringField) {
            return (Mixins.StringField) this;
        }
        return Mixins.asString(this, isNullable);
    }
}
