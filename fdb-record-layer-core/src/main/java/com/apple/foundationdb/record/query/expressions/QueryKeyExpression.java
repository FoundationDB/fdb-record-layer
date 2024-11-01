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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.planprotos.PComparison;
import com.apple.foundationdb.record.planprotos.PConversionParameterComparison;
import com.apple.foundationdb.record.planprotos.PConversionSimpleComparison;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.auto.service.AutoService;
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
            if (keyExpression.getComparandConversionFunction() != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionSimpleComparison(type, comparand, keyExpression));
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
            if (keyExpression.getComparandConversionFunction() != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionParameterComparison(type, param, keyExpression));
            } else {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
            }
        }
    }

    @Nonnull
    protected QueryKeyExpressionWithComparison simpleComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand) {
        if (keyExpression.getComparandConversionFunction() != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionSimpleComparison(type, comparand, keyExpression));
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
        if (keyExpression.getComparandConversionFunction() != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionParameterComparison(type, param, keyExpression));
        } else {
            return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
        }
    }

    private static final class ConversionSimpleComparison extends Comparisons.SimpleComparisonBase {
        private static final ObjectPlanHash CONVERSION_SIMPLE_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Simple-Comparison");
        @Nonnull
        private final QueryableKeyExpression keyExpression;
        @Nonnull
        private final Object unconvertedComparand;

        public ConversionSimpleComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand,
                                          @Nonnull QueryableKeyExpression keyExpression) {
            super(type, keyExpression.getComparandConversionFunction().apply(comparand));
            this.keyExpression = keyExpression;
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
                    return PlanHashable.objectsPlanHash(mode, CONVERSION_SIMPLE_COMPARISON_BASE_HASH, super.planHash(mode), getKeyExpression());
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, CONVERSION_SIMPLE_COMPARISON_BASE_HASH, super.queryHash(hashKind), getKeyExpression());
        }

        @Nonnull
        @Override
        public Comparisons.Comparison withType(@Nonnull final Comparisons.Type newType) {
            if (type == newType) {
                return this;
            }
            return new ConversionSimpleComparison(newType, unconvertedComparand, keyExpression);
        }

        @Nonnull
        @Override
        public PConversionSimpleComparison toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PConversionSimpleComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setObject(PlanSerialization.valueObjectToProto(unconvertedComparand))
                    .setConversion(keyExpression.toKeyExpression())
                    .build();
        }

        @Nonnull
        @Override
        public PComparison toComparisonProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setConversionSimpleComparison(toProto(serializationContext)).build();
        }

        @Nonnull
        public static ConversionSimpleComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                           @Nonnull final PConversionSimpleComparison simpleComparisonProto) {
            return new ConversionSimpleComparison(Comparisons.Type.fromProto(serializationContext, Objects.requireNonNull(simpleComparisonProto.getType())),
                    Objects.requireNonNull(PlanSerialization.protoToValueObject(Objects.requireNonNull(simpleComparisonProto.getObject()))),
                    (QueryableKeyExpression)KeyExpression.fromProto(simpleComparisonProto.getConversion()));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PConversionSimpleComparison, ConversionSimpleComparison> {
            @Nonnull
            @Override
            public Class<PConversionSimpleComparison> getProtoMessageClass() {
                return PConversionSimpleComparison.class;
            }

            @Nonnull
            @Override
            public ConversionSimpleComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PConversionSimpleComparison conversionSimpleComparisonProto) {
                return ConversionSimpleComparison.fromProto(serializationContext, conversionSimpleComparisonProto);
            }
        }
    }

    private static final class ConversionParameterComparison extends Comparisons.ParameterComparisonBase {
        private static final ObjectPlanHash CONVERSION_PARAMETER_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Parameter-Comparison");
        @Nonnull
        private final QueryableKeyExpression keyExpression;
        @Nonnull
        private final Function<Object, Object> conversion;

        protected ConversionParameterComparison(@Nonnull Comparisons.Type type, @Nonnull String parameter,
                                                @Nullable Bindings.BindingType bindingType,
                                                @Nonnull ParameterRelationshipGraph parameterRelationshipGraph,
                                                @Nonnull QueryableKeyExpression keyExpression) {
            super(type, parameter, bindingType, parameterRelationshipGraph);
            this.keyExpression = keyExpression;
            this.conversion = Objects.requireNonNull(keyExpression.getComparandConversionFunction());
        }

        public ConversionParameterComparison(@Nonnull Comparisons.Type type,
                                             @Nonnull String param,
                                             @Nonnull ParameterRelationshipGraph parameterRelationshipGraph,
                                             @Nonnull QueryableKeyExpression keyExpression) {
            this(type, param, null, parameterRelationshipGraph, keyExpression);
        }

        public ConversionParameterComparison(@Nonnull Comparisons.Type type,
                                             @Nonnull String param,
                                             @Nonnull QueryableKeyExpression keyExpression) {
            this(type, param, ParameterRelationshipGraph.unbound(), keyExpression);
        }

        @Nonnull
        @Override
        public Object getComparand(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context) {
            return conversion.apply(super.getComparand(store, context));
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
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

        @Nonnull
        @Override
        public Comparisons.Comparison withType(@Nonnull final Comparisons.Type newType) {
            if (type == newType) {
                return this;
            }
            return new ConversionParameterComparison(newType, parameter, bindingType, parameterRelationshipGraph, keyExpression);
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
                    return PlanHashable.objectsPlanHash(mode, CONVERSION_PARAMETER_COMPARISON_BASE_HASH, super.planHash(mode), getKeyExpression());
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, CONVERSION_PARAMETER_COMPARISON_BASE_HASH, super.queryHash(hashKind), getKeyExpression());
        }

        @Nonnull
        @Override
        protected Comparisons.ParameterComparisonBase withTranslatedCorrelation(@Nonnull CorrelationIdentifier translatedAlias) {
            return new ConversionParameterComparison(type,
                    Bindings.BindingType.CORRELATION.bindingName(translatedAlias.getId()),
                    Bindings.BindingType.CORRELATION,
                    parameterRelationshipGraph,
                    keyExpression);
        }

        @Nonnull
        @Override
        public Comparisons.Comparison withParameterRelationshipMap(@Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ConversionParameterComparison(type, parameter, bindingType, parameterRelationshipGraph, keyExpression);
        }

        @Nonnull
        @Override
        public PConversionParameterComparison toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PConversionParameterComparison.Builder builder = PConversionParameterComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setParameter(parameter)
                    .setConversion(keyExpression.toKeyExpression());
            if (bindingType != null) {
                builder.setInternal(bindingType.toProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        @Override
        public PComparison toComparisonProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setConversionParameterComparison(toProto(serializationContext)).build();
        }

        @Nonnull
        public static ConversionParameterComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                              @Nonnull final PConversionParameterComparison conversionParameterComparisonProto) {
            final Bindings.BindingType bindingType;
            if (conversionParameterComparisonProto.hasInternal()) {
                bindingType = Bindings.BindingType.fromProto(serializationContext, Objects.requireNonNull(conversionParameterComparisonProto.getInternal()));
            } else {
                bindingType = null;
            }
            final QueryableKeyExpression keyExpression = (QueryableKeyExpression)
                    KeyExpression.fromProto(conversionParameterComparisonProto.getConversion());
            return new ConversionParameterComparison(Comparisons.Type.fromProto(serializationContext, Objects.requireNonNull(conversionParameterComparisonProto.getType())),
                    Objects.requireNonNull(conversionParameterComparisonProto.getParameter()),
                    bindingType, ParameterRelationshipGraph.unbound(),
                    keyExpression);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PConversionParameterComparison, ConversionParameterComparison> {
            @Nonnull
            @Override
            public Class<PConversionParameterComparison> getProtoMessageClass() {
                return PConversionParameterComparison.class;
            }

            @Nonnull
            @Override
            public ConversionParameterComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                           @Nonnull final PConversionParameterComparison conversionParameterComparisonProto) {
                return ConversionParameterComparison.fromProto(serializationContext, conversionParameterComparisonProto);
            }
        }
    }

}
