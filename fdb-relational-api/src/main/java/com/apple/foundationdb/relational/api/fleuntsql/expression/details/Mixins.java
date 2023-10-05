/*
 * Mixins.java
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

package com.apple.foundationdb.relational.api.fleuntsql.expression.details;

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.fleuntsql.FluentVisitor;
import com.apple.foundationdb.relational.api.fleuntsql.expression.BooleanExpressionTrait;
import com.apple.foundationdb.relational.api.fleuntsql.expression.ExpressionFragment;
import com.apple.foundationdb.relational.api.fleuntsql.expression.Field;
import com.apple.foundationdb.relational.api.fleuntsql.expression.NumericExpressionTrait;

import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

public interface Mixins {

    interface FieldEqualityTrait<T extends DataType> extends Field<T> {
        default boolean equalsInternal(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (obj instanceof Field<?>) {
                // do not use mixin.equals(...), so we can equate this with other ExpressionType<?> objects
                final Field<?> other = (Field<?>) obj;
                return getType().equals(other.getType()) && getParts().equals(other.getParts());
            }

            return false;
        }
    }

    interface BooleanField extends FieldEqualityTrait<DataType.BooleanType>, BooleanExpressionTrait {}

    interface StringField extends FieldEqualityTrait<DataType.StringType> {}

    interface IntField extends FieldEqualityTrait<DataType.IntegerType>, NumericExpressionTrait<DataType.IntegerType> {
        @Override
        default DataType.IntegerType getType() {
            return DataType.IntegerType.nullable();
        }
    }

    interface LongField extends FieldEqualityTrait<DataType.LongType>, NumericExpressionTrait<DataType.LongType> {
        @Override
        default DataType.LongType getType() {
            return DataType.LongType.nullable();
        }
    }

    interface FloatField extends FieldEqualityTrait<DataType.FloatType>, NumericExpressionTrait<DataType.FloatType> {
        @Override
        default DataType.FloatType getType() {
            return DataType.FloatType.nullable();
        }
    }

    interface DoubleField extends FieldEqualityTrait<DataType.DoubleType>, NumericExpressionTrait<DataType.DoubleType> {
        @Override
        default DataType.DoubleType getType() {
            return DataType.DoubleType.nullable();
        }
    }

    @Nonnull
    static BooleanField asBoolean(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new BooleanField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.BooleanType getType() {
                return isNullable ? DataType.BooleanType.nullable() : DataType.BooleanType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    @Nonnull
    static IntField asInt(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new IntField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.IntegerType getType() {
                return isNullable ? DataType.IntegerType.nullable() : DataType.IntegerType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    @Nonnull
    static LongField asLong(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new LongField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.LongType getType() {
                return isNullable ? DataType.LongType.nullable() : DataType.LongType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    @Nonnull
    static FloatField asFloat(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new FloatField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.FloatType getType() {
                return isNullable ? DataType.FloatType.nullable() : DataType.FloatType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    @Nonnull
    static DoubleField asDouble(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new DoubleField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.DoubleType getType() {
                return isNullable ? DataType.DoubleType.nullable() : DataType.DoubleType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    @Nonnull
    static StringField asString(@Nonnull final Field<?> mixin, boolean isNullable) {
        return new StringField() {
            @Nonnull
            @SuppressWarnings("PMD.FieldNamingConventions")
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            @Override
            public Iterable<String> getParts() {
                return mixin.getParts();
            }

            @Nonnull
            @Override
            public Field<?> subField(@Nonnull String part) {
                return mixin.subField(part);
            }

            @Nonnull
            @Override
            public String getName() {
                return mixin.getName();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.StringType getType() {
                return isNullable ? DataType.StringType.nullable() : DataType.StringType.notNullable();
            }

            private int computeHashCode() {
                return mixin.hashCode();
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object other) {
                return equalsInternal(other);
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }
        };
    }

    interface ExpressionFragmentEqualityTrait<T extends DataType> extends ExpressionFragment<T> {
        default boolean equalsInternal(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (obj instanceof ExpressionFragment<?>) {
                // do not use mixin.equals(...), so we can equate this with other ExpressionType<?> objects
                final ExpressionFragment<?> other = (ExpressionFragment<?>) obj;
                return getType().equals(other.getType()) && getFragment().equals(other.getFragment());
            }

            return false;
        }
    }

    interface BooleanExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.BooleanType>, BooleanExpressionTrait {
        @Override
        default DataType.BooleanType getType() {
            return DataType.BooleanType.nullable();
        }
    }

    interface StringExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.StringType> {
        @Override
        default DataType.StringType getType() {
            return DataType.StringType.nullable();
        }
    }

    interface IntExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.IntegerType>, NumericExpressionTrait<DataType.IntegerType> {
        @Override
        default DataType.IntegerType getType() {
            return DataType.IntegerType.nullable();
        }
    }

    interface LongExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.LongType>, NumericExpressionTrait<DataType.LongType> {
        @Override
        default DataType.LongType getType() {
            return DataType.LongType.nullable();
        }
    }

    interface FloatExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.FloatType>, NumericExpressionTrait<DataType.FloatType> {
        @Override
        default DataType.FloatType getType() {
            return DataType.FloatType.nullable();
        }
    }

    interface DoubleExpressionFragment extends ExpressionFragmentEqualityTrait<DataType.DoubleType>, NumericExpressionTrait<DataType.DoubleType> {
        @Override
        default DataType.DoubleType getType() {
            return DataType.DoubleType.nullable();
        }
    }

    @Nonnull
    static BooleanExpressionFragment asBoolean(ExpressionFragment<?> mixin, boolean isNullable) {
        return new BooleanExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.BooleanType getType() {
                return isNullable ? DataType.BooleanType.nullable() : DataType.BooleanType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }

    @Nonnull
    static IntExpressionFragment asInt(ExpressionFragment<?> mixin, boolean isNullable) {
        return new IntExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.IntegerType getType() {
                return isNullable ? DataType.IntegerType.nullable() : DataType.IntegerType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }

    @Nonnull
    static LongExpressionFragment asLong(ExpressionFragment<?> mixin, boolean isNullable) {
        return new LongExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.LongType getType() {
                return isNullable ? DataType.LongType.nullable() : DataType.LongType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }

    @Nonnull
    static FloatExpressionFragment asFloat(ExpressionFragment<?> mixin, boolean isNullable) {
        return new FloatExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.FloatType getType() {
                return isNullable ? DataType.FloatType.nullable() : DataType.FloatType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }

    @Nonnull
    static DoubleExpressionFragment asDouble(ExpressionFragment<?> mixin, boolean isNullable) {
        return new DoubleExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.DoubleType getType() {
                return isNullable ? DataType.DoubleType.nullable() : DataType.DoubleType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }

    @Nonnull
    static StringExpressionFragment asString(ExpressionFragment<?> mixin, boolean isNullable) {
        return new StringExpressionFragment() {
            @Nonnull
            @Override
            public String getFragment() {
                return mixin.getFragment();
            }

            @Nullable
            @Override
            public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
                return mixin.accept(visitor, context);
            }

            @Override
            public DataType.StringType getType() {
                return isNullable ? DataType.StringType.nullable() : DataType.StringType.notNullable();
            }

            @Override
            public String toString() {
                return "(" + mixin + ") : " + getType();
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            @Override
            public boolean equals(Object obj) {
                return equalsInternal(obj);
            }

            @Override
            public int hashCode() {
                return mixin.hashCode();
            }
        };
    }
}
