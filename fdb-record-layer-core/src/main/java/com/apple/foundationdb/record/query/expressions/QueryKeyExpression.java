/*
 * QueryKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Allow use of a {@link QueryableKeyExpression} in a query.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryKeyExpression {
    private static final ObjectPlanHash SIMPLE_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Simple-Comparison");
    private static final ObjectPlanHash PARAMETER_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Parameter-Comparison");

    @Nonnull
    protected final QueryableKeyExpression keyExpression;

    public QueryKeyExpression(@Nonnull QueryableKeyExpression keyExpression) {
        this.keyExpression = keyExpression;
    }

    /**
     * Checks if the key expression has a value equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsValue(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value not equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notEquals(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.NOT_EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value greater than the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThan(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.GREATER_THAN, comparand);
    }

    /**
     * Checks if the key expression has a value greater than or equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThanOrEquals(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value less than the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThan(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.LESS_THAN, comparand);
    }

    /**
     * Checks if the key expression has a value less than or equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThanOrEquals(@Nonnull Object comparand) {
        return simpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand);
    }

    /**
     * Checks if the key expression starts with the given string.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent startsWith(@Nonnull String comparand) {
        return simpleComparison(Comparisons.Type.STARTS_WITH, comparand);
    }

    /**
     * Returns true if the key expression evaluates to {@code null}.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent isNull() {
        return nullComparison(Comparisons.Type.IS_NULL);
    }

    /**
     * Returns true if the key expression does not evaluate to {@code null}.
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notNull() {
        return nullComparison(Comparisons.Type.NOT_NULL);
    }

    /**
     * Checks if the key expression has a value equal to the given parameter.
     * @param param the name of the parameter
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsParameter(@Nonnull String param) {
        return parameterComparison(Comparisons.Type.EQUALS, param);
    }

    /**
     * Add comparisons to one of the values returned by a multi-valued expression.
     * @return a builder for comparisons
     */
    @Nonnull
    public OneOfThem oneOfThem() {
        return new OneOfThem();
    }

    /**
     * Allow comparisons against a member of a multi-valued expression.
     */
    public class OneOfThem {
        private OneOfThem() {
        }

        @Nonnull
        public QueryComponent equalsValue(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.EQUALS, comparand);
        }

        @Nonnull
        public QueryComponent notEquals(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.NOT_EQUALS, comparand);
        }

        @Nonnull
        public QueryComponent greaterThan(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.GREATER_THAN, comparand);
        }

        @Nonnull
        public QueryComponent greaterThanOrEquals(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand);
        }

        @Nonnull
        public QueryComponent lessThan(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.LESS_THAN, comparand);
        }

        @Nonnull
        public QueryComponent lessThanOrEquals(@Nonnull Object comparand) {
            return simpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand);
        }

        @Nonnull
        public QueryComponent startsWith(@Nonnull String comparand) {
            return simpleComparison(Comparisons.Type.STARTS_WITH, comparand);
        }

        @Nonnull
        public QueryComponent isNull() {
            return nullComparison(Comparisons.Type.IS_NULL);
        }

        @Nonnull
        public QueryComponent notNull() {
            return nullComparison(Comparisons.Type.NOT_NULL);
        }

        @Nonnull
        public QueryComponent equalsParameter(@Nonnull String param) {
            return parameterComparison(Comparisons.Type.EQUALS, param);
        }

        @Nonnull
        private QueryKeyExpressionWithOneOfComparison simpleComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand) {
            final Function<Object, Object> conversion = keyExpression.getComparandConversionFunction();
            if (conversion != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionSimpleComparison(type, comparand, conversion));
            } else {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.SimpleComparison(type, comparand));
            }
        }

        @Nonnull
        private QueryKeyExpressionWithOneOfComparison nullComparison(@Nonnull Comparisons.Type type) {
            return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.NullComparison(type));
        }

        @Nonnull
        private QueryKeyExpressionWithOneOfComparison parameterComparison(@Nonnull Comparisons.Type type, @Nonnull String param) {
            final Function<Object, Object> conversion = keyExpression.getComparandConversionFunction();
            if (conversion != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionParameterComparison(type, param, conversion));
            } else {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
            }
        }
    }

    @Nonnull
    protected QueryKeyExpressionWithComparison simpleComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand) {
        final Function<Object, Object> conversion = keyExpression.getComparandConversionFunction();
        if (conversion != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionSimpleComparison(type, comparand, conversion));
        } else {
            return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.SimpleComparison(type, comparand));
        }
    }

    @Nonnull
    private QueryKeyExpressionWithComparison nullComparison(@Nonnull Comparisons.Type type) {
        return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.NullComparison(type));
    }

    @Nonnull
    protected QueryKeyExpressionWithComparison parameterComparison(@Nonnull Comparisons.Type type, @Nonnull String param) {
        final Function<Object, Object> conversion = keyExpression.getComparandConversionFunction();
        if (conversion != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionParameterComparison(type, param, conversion));
        } else {
            return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
        }
    }

    private final class ConversionSimpleComparison extends Comparisons.SimpleComparison {
        @Nonnull
        private final Function<Object, Object> conversion;
        @Nonnull
        private final Object unconvertedComparand;

        public ConversionSimpleComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand,
                                          @Nonnull Function<Object, Object> conversion) {
            super(type, conversion.apply(comparand));
            this.conversion = conversion;
            this.unconvertedComparand = comparand;
        }

        @Nonnull
        private QueryableKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Nonnull
        @Override
        public String typelessString() {
            return getKeyExpression().getName() + "(" + Comparisons.toPrintable(unconvertedComparand) + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ConversionSimpleComparison that = (ConversionSimpleComparison)o;
            return getKeyExpression().equals(that.getKeyExpression());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getKeyExpression());
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return super.planHash(mode) + getKeyExpression().planHash(mode);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, SIMPLE_COMPARISON_BASE_HASH, super.planHash(mode), getKeyExpression());
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, SIMPLE_COMPARISON_BASE_HASH, super.queryHash(hashKind), getKeyExpression());
        }
    }

    private final class ConversionParameterComparison extends Comparisons.ParameterComparison {
        @Nonnull
        private final Function<Object, Object> conversion;

        public ConversionParameterComparison(@Nonnull Comparisons.Type type,
                                             @Nonnull String param,
                                             @Nonnull Function<Object, Object> conversion) {
            this(type, param, ParameterRelationshipGraph.unbound(), conversion);
        }

        public ConversionParameterComparison(@Nonnull Comparisons.Type type,
                                             @Nonnull String param,
                                             @Nonnull ParameterRelationshipGraph parameterRelationshipGraph,
                                             @Nonnull Function<Object, Object> conversion) {
            super(type, param, null, parameterRelationshipGraph);
            this.conversion = conversion;
        }

        @Nonnull
        @Override
        public Object getComparand(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context) {
            return conversion.apply(super.getComparand(store, context));
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            final Object comparand = context.getBinding(parameter);
            if (comparand == null) {
                return null;
            }
            return Comparisons.evalComparison(getType(), value, conversion.apply(comparand));
        }

        @Nonnull
        private QueryableKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Nonnull
        @Override
        public String typelessString() {
            return getKeyExpression().getName() + "(" + super.typelessString() + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ConversionParameterComparison that = (ConversionParameterComparison)o;
            return getKeyExpression().equals(that.getKeyExpression());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getKeyExpression());
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return super.planHash(mode) + getKeyExpression().planHash(mode);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, PARAMETER_COMPARISON_BASE_HASH, super.planHash(mode), getKeyExpression());
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, PARAMETER_COMPARISON_BASE_HASH, super.queryHash(hashKind), getKeyExpression());
        }

        @Nonnull
        @Override
        public Comparisons.Comparison withParameterRelationshipMap(@Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ConversionParameterComparison(getType(), parameter, parameterRelationshipGraph, conversion);
        }
    }

}
