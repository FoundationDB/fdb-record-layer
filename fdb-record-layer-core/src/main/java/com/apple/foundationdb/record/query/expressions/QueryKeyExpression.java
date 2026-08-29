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
import com.apple.foundationdb.record.Bindings.Internal;
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
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Function;

/**
 * Allow use of a {@link QueryableKeyExpression} in a query.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryKeyExpression {
    protected final QueryableKeyExpression keyExpression;

    public QueryKeyExpression(QueryableKeyExpression keyExpression) {
        this.keyExpression = keyExpression;
    }

    /**
     * Checks if the key expression has a value equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent equalsValue(Object comparand) {
        return simpleComparison(Comparisons.Type.EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value not equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent notEquals(Object comparand) {
        return simpleComparison(Comparisons.Type.NOT_EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value greater than the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent greaterThan(Object comparand) {
        return simpleComparison(Comparisons.Type.GREATER_THAN, comparand);
    }

    /**
     * Checks if the key expression has a value greater than or equal to the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent greaterThanOrEquals(Object comparand) {
        return simpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand);
    }

    /**
     * Checks if the key expression has a value less than the given comparand.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent lessThan(Object comparand) {
        return simpleComparison(Comparisons.Type.LESS_THAN, comparand);
    }

    /**
     * Checks if the key expression has a value less than or equal to the given comparand.
     * Evaluates to null if the field does not have a value.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent lessThanOrEquals(Object comparand) {
        return simpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand);
    }

    /**
     * Checks if the key expression starts with the given string.
     * @param comparand the object to compare with the value in the field
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent startsWith(String comparand) {
        return simpleComparison(Comparisons.Type.STARTS_WITH, comparand);
    }

    /**
     * Returns true if the key expression evaluates to {@code null}.
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent isNull() {
        return nullComparison(Comparisons.Type.IS_NULL);
    }

    /**
     * Returns true if the key expression does not evaluate to {@code null}.
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent notNull() {
        return nullComparison(Comparisons.Type.NOT_NULL);
    }

    /**
     * Checks if the key expression has a value equal to the given parameter.
     * @param param the name of the parameter
     * @return a new component for doing the actual evaluation
     */
    public QueryComponent equalsParameter(String param) {
        return parameterComparison(Comparisons.Type.EQUALS, param);
    }

    /**
     * Add comparisons to one of the values returned by a multi-valued expression.
     * @return a builder for comparisons
     */
    public OneOfThem oneOfThem() {
        return new OneOfThem();
    }

    /**
     * Allow comparisons against a member of a multi-valued expression.
     */
    public class OneOfThem {
        private OneOfThem() {
        }

        public QueryComponent equalsValue(Object comparand) {
            return simpleComparison(Comparisons.Type.EQUALS, comparand);
        }

        public QueryComponent notEquals(Object comparand) {
            return simpleComparison(Comparisons.Type.NOT_EQUALS, comparand);
        }

        public QueryComponent greaterThan(Object comparand) {
            return simpleComparison(Comparisons.Type.GREATER_THAN, comparand);
        }

        public QueryComponent greaterThanOrEquals(Object comparand) {
            return simpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand);
        }

        public QueryComponent lessThan(Object comparand) {
            return simpleComparison(Comparisons.Type.LESS_THAN, comparand);
        }

        public QueryComponent lessThanOrEquals(Object comparand) {
            return simpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand);
        }

        public QueryComponent startsWith(String comparand) {
            return simpleComparison(Comparisons.Type.STARTS_WITH, comparand);
        }

        public QueryComponent isNull() {
            return nullComparison(Comparisons.Type.IS_NULL);
        }

        public QueryComponent notNull() {
            return nullComparison(Comparisons.Type.NOT_NULL);
        }

        public QueryComponent equalsParameter(String param) {
            return parameterComparison(Comparisons.Type.EQUALS, param);
        }

        private QueryKeyExpressionWithOneOfComparison simpleComparison(Comparisons.Type type, Object comparand) {
            if (keyExpression.getComparandConversionFunction() != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionSimpleComparison(type, comparand, keyExpression));
            } else {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.SimpleComparison(type, comparand));
            }
        }

        private QueryKeyExpressionWithOneOfComparison nullComparison(Comparisons.Type type) {
            return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.NullComparison(type));
        }

        private QueryKeyExpressionWithOneOfComparison parameterComparison(Comparisons.Type type, String param) {
            if (keyExpression.getComparandConversionFunction() != null) {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new ConversionParameterComparison(type, param, keyExpression));
            } else {
                return new QueryKeyExpressionWithOneOfComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
            }
        }
    }

    protected QueryKeyExpressionWithComparison simpleComparison(Comparisons.Type type, Object comparand) {
        if (keyExpression.getComparandConversionFunction() != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionSimpleComparison(type, comparand, keyExpression));
        } else {
            return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.SimpleComparison(type, comparand));
        }
    }

    private QueryKeyExpressionWithComparison nullComparison(Comparisons.Type type) {
        return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.NullComparison(type));
    }

    protected QueryKeyExpressionWithComparison parameterComparison(Comparisons.Type type, String param) {
        if (keyExpression.getComparandConversionFunction() != null) {
            return new QueryKeyExpressionWithComparison(keyExpression, new ConversionParameterComparison(type, param, keyExpression));
        } else {
            return new QueryKeyExpressionWithComparison(keyExpression, new Comparisons.ParameterComparison(type, param));
        }
    }

    private static final class ConversionSimpleComparison extends Comparisons.SimpleComparisonBase {
        private static final ObjectPlanHash CONVERSION_SIMPLE_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Simple-Comparison");
        private final QueryableKeyExpression keyExpression;
        private final Object unconvertedComparand;

        public ConversionSimpleComparison(Comparisons.Type type, Object comparand,
                                          QueryableKeyExpression keyExpression) {
            super(type, keyExpression.getComparandConversionFunction().apply(comparand));
            this.keyExpression = keyExpression;
            this.unconvertedComparand = comparand;
        }

        private QueryableKeyExpression getKeyExpression() {
            return keyExpression;
        }

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
        public int planHash(final PlanHashMode mode) {
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
        public Comparisons.Comparison withType(final Comparisons.Type newType) {
            if (type == newType) {
                return this;
            }
            return new ConversionSimpleComparison(newType, unconvertedComparand, keyExpression);
        }

        @Override
        public PConversionSimpleComparison toProto(final PlanSerializationContext serializationContext) {
            return PConversionSimpleComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setObject(PlanSerialization.valueObjectToProto(unconvertedComparand))
                    .setConversion(keyExpression.toKeyExpression())
                    .build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setConversionSimpleComparison(toProto(serializationContext)).build();
        }

        public static ConversionSimpleComparison fromProto(final PlanSerializationContext serializationContext,
                                                           final PConversionSimpleComparison simpleComparisonProto) {
            return new ConversionSimpleComparison(Comparisons.Type.fromProto(serializationContext, Objects.requireNonNull(simpleComparisonProto.getType())),
                    Objects.requireNonNull(PlanSerialization.protoToValueObject(Objects.requireNonNull(simpleComparisonProto.getObject()))),
                    (QueryableKeyExpression)KeyExpression.fromProto(simpleComparisonProto.getConversion()));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PConversionSimpleComparison, ConversionSimpleComparison> {
            @Override
            public Class<PConversionSimpleComparison> getProtoMessageClass() {
                return PConversionSimpleComparison.class;
            }

            @Override
            public ConversionSimpleComparison fromProto(final PlanSerializationContext serializationContext,
                                                        final PConversionSimpleComparison conversionSimpleComparisonProto) {
                return ConversionSimpleComparison.fromProto(serializationContext, conversionSimpleComparisonProto);
            }
        }
    }

    private static final class ConversionParameterComparison extends Comparisons.ParameterComparisonBase {
        private static final ObjectPlanHash CONVERSION_PARAMETER_COMPARISON_BASE_HASH = new ObjectPlanHash("Conversion-Parameter-Comparison");
        private final QueryableKeyExpression keyExpression;
        private final Function<Object, Object> conversion;

        protected ConversionParameterComparison(Comparisons.Type type, String parameter,
                                                @Nullable Internal internal,
                                                ParameterRelationshipGraph parameterRelationshipGraph,
                                                QueryableKeyExpression keyExpression) {
            super(type, parameter, internal, parameterRelationshipGraph);
            this.keyExpression = keyExpression;
            this.conversion = Objects.requireNonNull(keyExpression.getComparandConversionFunction());
        }

        public ConversionParameterComparison(Comparisons.Type type,
                                             String param,
                                             ParameterRelationshipGraph parameterRelationshipGraph,
                                             QueryableKeyExpression keyExpression) {
            this(type, param, null, parameterRelationshipGraph, keyExpression);
        }

        public ConversionParameterComparison(Comparisons.Type type,
                                             String param,
                                             QueryableKeyExpression keyExpression) {
            this(type, param, ParameterRelationshipGraph.unbound(), keyExpression);
        }

        @Override
        public Object getComparand(FDBRecordStoreBase<?> store, EvaluationContext context) {
            return conversion.apply(super.getComparand(store, context));
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            final Object comparand = context.getBinding(parameter);
            if (comparand == null) {
                return null;
            }
            return Comparisons.evalComparison(getType(), value, conversion.apply(comparand));
        }

        private QueryableKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Override
        public String typelessString() {
            return getKeyExpression().getName() + "(" + super.typelessString() + ")";
        }

        @Override
        public Comparisons.Comparison withType(final Comparisons.Type newType) {
            if (type == newType) {
                return this;
            }
            return new ConversionParameterComparison(newType, parameter, internal, parameterRelationshipGraph, keyExpression);
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
        public int planHash(final PlanHashMode mode) {
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
        protected Comparisons.ParameterComparisonBase withTranslatedCorrelation(CorrelationIdentifier translatedAlias) {
            return new ConversionParameterComparison(type,
                    Bindings.Internal.CORRELATION.bindingName(translatedAlias.getId()),
                    Bindings.Internal.CORRELATION,
                    parameterRelationshipGraph,
                    keyExpression);
        }

        @Override
        public Comparisons.Comparison withParameterRelationshipMap(final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ConversionParameterComparison(type, parameter, internal, parameterRelationshipGraph, keyExpression);
        }

        @Override
        public PConversionParameterComparison toProto(final PlanSerializationContext serializationContext) {
            final PConversionParameterComparison.Builder builder = PConversionParameterComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setParameter(parameter)
                    .setConversion(keyExpression.toKeyExpression());
            if (internal != null) {
                builder.setInternal(internal.toProto(serializationContext));
            }
            return builder.build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setConversionParameterComparison(toProto(serializationContext)).build();
        }

        public static ConversionParameterComparison fromProto(final PlanSerializationContext serializationContext,
                                                              final PConversionParameterComparison conversionParameterComparisonProto) {
            final Bindings.Internal internal;
            if (conversionParameterComparisonProto.hasInternal()) {
                internal = Bindings.Internal.fromProto(serializationContext, Objects.requireNonNull(conversionParameterComparisonProto.getInternal()));
            } else {
                internal = null;
            }
            final QueryableKeyExpression keyExpression = (QueryableKeyExpression)
                    KeyExpression.fromProto(conversionParameterComparisonProto.getConversion());
            return new ConversionParameterComparison(Comparisons.Type.fromProto(serializationContext, Objects.requireNonNull(conversionParameterComparisonProto.getType())),
                    Objects.requireNonNull(conversionParameterComparisonProto.getParameter()),
                    internal, ParameterRelationshipGraph.unbound(),
                    keyExpression);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PConversionParameterComparison, ConversionParameterComparison> {
            @Override
            public Class<PConversionParameterComparison> getProtoMessageClass() {
                return PConversionParameterComparison.class;
            }

            @Override
            public ConversionParameterComparison fromProto(final PlanSerializationContext serializationContext,
                                                           final PConversionParameterComparison conversionParameterComparisonProto) {
                return ConversionParameterComparison.fromProto(serializationContext, conversionParameterComparisonProto);
            }
        }
    }

}
