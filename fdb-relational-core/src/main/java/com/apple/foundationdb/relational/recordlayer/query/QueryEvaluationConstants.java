/*
 * QueryEvaluationConstants.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.continuation.TypedEvaluationConstant;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A container of multiple {@link QueryEvaluationConstant}s for a query.
 */
public final class QueryEvaluationConstants {
    @Nonnull
    private static final QueryEvaluationConstants EMPTY = new QueryEvaluationConstants(ImmutableSet.of());

    @Nonnull
    private final Set<QueryEvaluationConstant> evaluationConstants;

    @Nonnull
    private final Supplier<Map<String, Object>> asMapByConstantIdSupplier;

    private QueryEvaluationConstants(@Nonnull final Set<QueryEvaluationConstant> evaluationConstants) {
        this.evaluationConstants = ImmutableSet.copyOf(evaluationConstants);
        this.asMapByConstantIdSupplier = Suppliers.memoize(() ->
                this.evaluationConstants
                        .stream()
                        .collect(ImmutableMap.<QueryEvaluationConstant, String, Object>toImmutableMap(
                                queryEvaluationConstant -> queryEvaluationConstant.constantKind.constantId,
                                QueryEvaluationConstant::getValue)));
    }

    public static QueryEvaluationConstants.Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public Stream<QueryEvaluationConstant> stream() {
        return evaluationConstants.stream();
    }

    public boolean isEmpty() {
        return evaluationConstants.isEmpty();
    }

    /**
     * Retrieve a map with the values of the constants contained in this instance, keyed by the unique
     * constant ID string.
     * @return a map from the unique evaluation constant ID string to the value of this constant.
     */
    @Nonnull
    public Map<String, Object> asMapByConstantId() {
        return asMapByConstantIdSupplier.get();
    }

    @Nonnull
    public static QueryEvaluationConstants empty() {
        return EMPTY;
    }

    public static final class Builder {
        @Nonnull
        private final Map<QueryEvaluationConstant.ConstantKind, QueryEvaluationConstant> constants;

        private Builder() {
            this.constants = new EnumMap<>(QueryEvaluationConstant.ConstantKind.class);
        }

        /**
         * Set the value for the {@link QueryEvaluationConstant.ConstantKind#EVALUATION_TIMESTAMP} constant.
         * <p>
         * If the evaluation timestamp was previously set, this would overwrite the previous value.
         *
         * @param evaluationTimestamp an instant representing the evaluation timestamp.
         * @return this builder.
         */
        public Builder setEvaluationTimestamp(@Nonnull Instant evaluationTimestamp) {
            return addEvaluationConstant(
                    new QueryEvaluationConstant(
                            QueryEvaluationConstant.ConstantKind.EVALUATION_TIMESTAMP,
                            evaluationTimestamp.toEpochMilli()));
        }

        /**
         * Add a {@link QueryEvaluationConstant} to this builder.
         * <p>
         * If another {@link QueryEvaluationConstant} of the same {@link QueryEvaluationConstant.ConstantKind}
         * was previously added, this would overwrite the previous value.
         *
         * @param toAdd the constant to add.
         * @return this builder.
         */
        public Builder addEvaluationConstant(@Nonnull QueryEvaluationConstant toAdd) {
            constants.put(toAdd.getKind(), toAdd);
            return this;
        }

        @Nonnull
        public QueryEvaluationConstants build() {
            return new QueryEvaluationConstants(ImmutableSet.copyOf(constants.values()));
        }
    }

    /**
     * A constant value derived from the <b>evaluation environment</b>, which can be stored in a query's
     * {@link com.apple.foundationdb.relational.recordlayer.query.Plan.ExecutionContext} and depended on by different
     * {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} in a physical plan to ensure they
     * all evaluate to the same constant value.
     * <p>
     * An example usage of this is the current timestamp returned by the {@code CURRENT_TIMESTAMP} SQL function,
     * which should be the same across all invocations of the function in the same query.
     * <p>
     * Instances of this class can only be created by the different methods of {@link QueryEvaluationConstants.Builder}
     * or via {@link QueryEvaluationConstant#fromProto}.
     */
    public static final class QueryEvaluationConstant {
        /**
         * The supported kinds of query evaluation constants and their properties.
         */
        public enum ConstantKind {
            EVALUATION_TIMESTAMP("__evaluation_timestamp", Type.primitiveType(Type.TypeCode.LONG));

            @Nonnull
            private final String constantId;

            @Nonnull
            private final Type dataType;

            @Nonnull
            private final ConstantObjectValue constantObjectValue;

            ConstantKind(@Nonnull String constantId, @Nonnull Type dataType) {
                this.constantId = constantId;
                this.dataType = dataType;
                this.constantObjectValue = ConstantObjectValue.of(Quantifier.constant(), constantId, dataType);
            }

            /**
             * Get a {@link ConstantObjectValue} which references this constant in an {@link com.apple.foundationdb.record.EvaluationContext}.
             *
             * @return an instance of {@link ConstantObjectValue} that corresponds to this evaluation constant.
             */
            @Nonnull
            public ConstantObjectValue getConstantObjectValue() {
                return constantObjectValue;
            }

            @Nonnull
            private static Optional<ConstantKind> fromConstantIdAndType(@Nonnull String constantId, @Nonnull Type dataType) {
                return Arrays.stream(values())
                        .filter(constantKind ->
                                constantKind.constantId.equals(constantId) && constantKind.dataType.equals(dataType))
                        .findFirst();
            }
        }

        @Nonnull
        private final ConstantKind constantKind;

        @Nullable
        private final Object value;

        @VisibleForTesting
        QueryEvaluationConstant(@Nonnull ConstantKind constantKind, @Nullable Object value) {
            this.constantKind = constantKind;
            this.value = value;
        }

        /**
         * Get the {@link ConstantKind} of this constant instance.
         *
         * @return the {@link ConstantKind} of this constant instance.
         */
        @Nonnull
        public ConstantKind getKind() {
            return constantKind;
        }

        @Nullable
        public Object getValue() {
            return value;
        }

        @Nonnull
        public TypedEvaluationConstant toProto(@Nonnull PlanSerializationContext serializationContext) {
            final var dataType = constantKind.dataType;
            return TypedEvaluationConstant.newBuilder()
                    .setType(dataType.toTypeProto(serializationContext))
                    .setConstantId(constantKind.constantId)
                    .setObject(LiteralsUtils.objectToLiteralObjectProto(dataType, value))
                    .build();
        }

        @Nonnull
        public static QueryEvaluationConstant fromProto(@Nonnull PlanSerializationContext serializationContext,
                                                        @Nonnull TypeRepository typeRepository,
                                                        @Nonnull TypedEvaluationConstant typedEvaluationConstant) {
            final var dataType = Type.fromTypeProto(serializationContext, typedEvaluationConstant.getType());
            final var constantKind = Assert.optionalUnchecked(
                    ConstantKind.fromConstantIdAndType(typedEvaluationConstant.getConstantId(), dataType),
                    ErrorCode.INTERNAL_ERROR,
                    () -> "unsupported query evaluation constant with ID " + typedEvaluationConstant.getConstantId() + " and type " + dataType.getTypeCode().toString());
            final var value = LiteralsUtils.objectFromLiteralObjectProto(typeRepository, dataType, typedEvaluationConstant.getObject());

            return new QueryEvaluationConstant(constantKind, value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof final QueryEvaluationConstant that)) {
                return false;
            }
            return constantKind == that.constantKind && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constantKind, value);
        }

        @Override
        public String toString() {
            return constantKind + "=" + value;
        }
    }
}
