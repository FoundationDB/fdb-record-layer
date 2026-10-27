/*
 * DoubleValueOrParameter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PDoubleValueOrParameter;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * A double-valued scalar sourced from a literal, a named binding on the {@link EvaluationContext}, or a Cascades
 * {@link Value} expression. Used by geospatial R-tree scans to carry the center coordinates and radius (each of which
 * may be a query-time parameter or a correlated sub-expression) through plan serialization and
 * {@link com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanComparisons#bind bind-time} evaluation.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class DoubleValueOrParameter implements PlanHashable {
    /**
     * Get the current value from the query bindings.
     * @param context the query context
     * @return a double value or {@code null}.
     */
    public abstract Double getValue(@Nonnull EvaluationContext context);

    /**
     * Resolve the value with access to a record store, when the underlying source is a Cascades {@link Value} that may
     * require store-scoped resolution. Legacy literal- and parameter-backed subtypes ignore {@code store}.
     * @param store the record store the containing scan is being evaluated against, or {@code null}
     * @param context the query context
     * @return a double value or {@code null}
     */
    public Double getValue(@javax.annotation.Nullable FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context) {
        return getValue(context);
    }

    /**
     * Correlations this source depends on. Literal- and parameter-backed subtypes have no correlations; a
     * {@link Value}-backed subtype forwards to its underlying value.
     */
    @Nonnull
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    /**
     * Return a copy of this source with any correlations rewritten through {@code translationMap}. Default returns
     * {@code this}; only {@link Value}-backed subtypes need translation.
     */
    @Nonnull
    public DoubleValueOrParameter translateCorrelations(@Nonnull TranslationMap translationMap,
                                                        boolean shouldSimplifyValues) {
        return this;
    }

    /**
     * Alias-aware equality; defers to {@link #equals(Object)} for subtypes with no correlations.
     */
    public boolean semanticEquals(@javax.annotation.Nullable Object other, @Nonnull AliasMap aliasMap) {
        return equals(other);
    }

    /**
     * Alias-independent hash, symmetric with {@link #semanticEquals(Object, AliasMap)}.
     */
    public int semanticHashCode() {
        return hashCode();
    }

    /**
     * Serialize this value or parameter reference.
     * @param serializationContext the serialization context
     * @return the proto message
     */
    @Nonnull
    public abstract PDoubleValueOrParameter toProto(@Nonnull PlanSerializationContext serializationContext);

    /**
     * Project this source into a Cascades {@link Value}. Literal sources become a {@link LiteralValue};
     * named-parameter sources become a nullable-double {@link ParameterObjectValue}; value-backed sources return their
     * underlying expression. Used by {@link GeospatialWithinDistanceComponent#expand} to bridge the QueryComponent
     * form into a Cascades {@link com.apple.foundationdb.record.query.plan.cascades.GraphExpansion}.
     */
    @Nonnull
    public abstract Value toValue();

    /**
     * Deserialize a value or parameter reference.
     * @param serializationContext the serialization context
     * @param proto the proto message
     * @return the reconstructed value or parameter reference
     */
    @Nonnull
    public static DoubleValueOrParameter fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PDoubleValueOrParameter proto) {
        switch (proto.getKindCase()) {
            case VALUE:
                return value(proto.getValue());
            case PARAMETER:
                return parameter(proto.getParameter());
            case VALUE_EXPRESSION:
                return valueExpression(Value.fromValueProto(serializationContext,
                        Objects.requireNonNull(proto.getValueExpression())));
            default:
                throw new RecordCoreException("unknown double value or parameter kind");
        }
    }

    /**
     * Get a coordinate for a constant value.
     * @param value the coordinate value
     * @return a new coordinate using the given value
     */
    @Nonnull
    public static DoubleValueOrParameter value(double value) {
        return new DoubleValue(value);
    }

    /**
     * Get a coordinate for a parameterized value.
     * @param parameter the parameter name
     * @return a new coordinate using the given parameter
     */
    @Nonnull
    public static DoubleValueOrParameter parameter(@Nonnull String parameter) {
        return new DoubleParameter(parameter);
    }

    /**
     * Get a coordinate backed by a Cascades {@link Value} expression, evaluated against the store and evaluation
     * context at bind time. Enables correlated sub-expressions (e.g. a quantified reference bound elsewhere in the
     * plan graph) as center/radius sources when a geospatial scan is chosen by the Cascades planner.
     * @param valueExpression a scalar value expression whose evaluated result is a {@link Double}
     * @return a new coordinate delegating to {@code valueExpression}
     */
    @Nonnull
    public static DoubleValueOrParameter valueExpression(@Nonnull Value valueExpression) {
        return new DoubleFromValue(valueExpression);
    }

    static class DoubleValue extends DoubleValueOrParameter {
        private final double value;

        DoubleValue(double value) {
            this.value = value;
        }

        @Override
        public Double getValue(@Nonnull EvaluationContext context) {
            return value;
        }

        @Nonnull
        @Override
        public Value toValue() {
            return LiteralValue.ofScalar(value);
        }

        @Nonnull
        @Override
        public PDoubleValueOrParameter toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PDoubleValueOrParameter.newBuilder().setValue(value).build();
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return Double.hashCode(value);
        }

        @Override
        public String toString() {
            return Double.toString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleValue that = (DoubleValue)o;
            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    static class DoubleParameter extends DoubleValueOrParameter {
        @Nonnull
        private final String parameter;

        DoubleParameter(@Nonnull String parameter) {
            this.parameter = parameter;
        }

        @Override
        public Double getValue(@Nonnull EvaluationContext context) {
            return (Double)context.getBinding(parameter);
        }

        @Nonnull
        @Override
        public Value toValue() {
            return ParameterObjectValue.of(parameter, Type.primitiveType(Type.TypeCode.DOUBLE));
        }

        @Nonnull
        @Override
        public PDoubleValueOrParameter toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PDoubleValueOrParameter.newBuilder().setParameter(parameter).build();
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return parameter.hashCode();
        }

        @Override
        public String toString() {
            return "$" + parameter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleParameter that = (DoubleParameter)o;
            return parameter.equals(that.parameter);
        }

        @Override
        public int hashCode() {
            return parameter.hashCode();
        }
    }

    /**
     * A {@link DoubleValueOrParameter} sourced from a Cascades {@link Value}. Distinguished from
     * {@link DoubleParameter} by carrying its own correlations and participating in
     * {@link #translateCorrelations(TranslationMap, boolean) correlation translation}; distinguished from
     * {@link DoubleValue} by requiring runtime evaluation against store+context. Consumed by
     * {@link com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanComparisons} to let Cascades-planned
     * geospatial queries carry bound query parameters as their center/radius sources.
     */
    static class DoubleFromValue extends DoubleValueOrParameter {
        @Nonnull
        private final Value valueExpression;

        DoubleFromValue(@Nonnull Value valueExpression) {
            this.valueExpression = valueExpression;
        }

        @Nonnull
        @Override
        public Value toValue() {
            return valueExpression;
        }

        @Override
        public Double getValue(@Nonnull EvaluationContext context) {
            return getValue(null, context);
        }

        @Override
        public Double getValue(@javax.annotation.Nullable FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context) {
            final Object evaluated = valueExpression.eval(store, context);
            if (evaluated == null) {
                return null;
            }
            if (evaluated instanceof Number) {
                return ((Number)evaluated).doubleValue();
            }
            throw new RecordCoreException("value expression did not evaluate to a numeric result");
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return valueExpression.getCorrelatedTo();
        }

        @Nonnull
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public DoubleValueOrParameter translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                            final boolean shouldSimplifyValues) {
            final Value translated = valueExpression.translateCorrelations(translationMap, shouldSimplifyValues);
            if (translated == valueExpression) {
                return this;
            }
            return new DoubleFromValue(translated);
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(@javax.annotation.Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DoubleFromValue)) {
                return false;
            }
            return valueExpression.semanticEquals(((DoubleFromValue)other).valueExpression, aliasMap);
        }

        @Override
        public int semanticHashCode() {
            return valueExpression.semanticHashCode();
        }

        @Nonnull
        @Override
        public PDoubleValueOrParameter toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PDoubleValueOrParameter.newBuilder()
                    .setValueExpression(valueExpression.toValueProto(serializationContext))
                    .build();
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return PlanHashable.objectPlanHash(mode, valueExpression);
        }

        @Override
        public String toString() {
            return valueExpression.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleFromValue that = (DoubleFromValue)o;
            return valueExpression.equals(that.valueExpression);
        }

        @Override
        public int hashCode() {
            return valueExpression.hashCode();
        }
    }
}
