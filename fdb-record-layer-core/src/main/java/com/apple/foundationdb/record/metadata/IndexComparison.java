/*
 * IndexComparison.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * This is a simple PoJo hierarchy representing SerDe operations on a predicate comparison of a sparse {@link Index}.
 * It resembles very closely a subset of {@link Comparisons.Comparison} type hierarchy, and offers a way of conversion to
 * {@link Comparisons.Comparison} (see {@link IndexComparison#toComparison()}).
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexComparison {

    /**
     * Parses a {@link com.apple.foundationdb.record.RecordMetaDataProto.Comparison} message into a corresponding
     * {@link IndexComparison} object.
     *
     * @param proto the protobuf message.
     * @return an equivalent {@link IndexComparison} object.
     * @throws RecordCoreException if the provided message is not supported.
     */
    @Nonnull
    public static IndexComparison fromProto(@Nonnull final RecordMetaDataProto.Comparison proto) {
        if (proto.hasSimpleComparison()) {
            return new SimpleComparison(proto.getSimpleComparison());
        } else if (proto.hasNullComparison()) {
            return new NullComparison(proto.getNullComparison());
        }
        throw new RecordCoreException("attempt to deserialize unsupported comparison").addLogInfo(LogMessageKeys.COMPARISON_VALUE, proto);
    }

    /**
     * Returns an equivalent {@link IndexComparison} object from a {@link Comparisons.Comparison}.
     *
     * @param comparison the comparison.
     * @return an equivalent {@link IndexComparison} object.
     * @throws RecordCoreException if the provided {@link Comparisons.Comparison} is not supported.
     */
    @VisibleForTesting
    @Nonnull
    public static IndexComparison fromComparison(@Nonnull final Comparisons.Comparison comparison) {
        if (comparison instanceof Comparisons.SimpleComparison) {
            return new SimpleComparison((Comparisons.SimpleComparison)comparison);
        } else if (comparison instanceof Comparisons.NullComparison) {
            return new NullComparison((Comparisons.NullComparison)comparison);
        } else {
            throw new RecordCoreException("attempt to create PoJo index comparison from unsupported comparison").addLogInfo(LogMessageKeys.COMPARISON_VALUE, comparison);
        }
    }

    /**
     * Checks whether a {@link Comparisons.Comparison} can be represented by this POJO hierarchy.
     *
     * @param comparison The comparison to check.
     * @return {@code true} if the comparison is supported, otherwise {@code false}.
     */
    public static boolean isSupported(@Nonnull final Comparisons.Comparison comparison) {
        return comparison instanceof Comparisons.SimpleComparison ||
                comparison instanceof Comparisons.NullComparison ||
                (comparison instanceof Comparisons.ValueComparison &&
                         ((Comparisons.ValueComparison)comparison).getComparandValue().preOrderStream()
                                 .filter(value -> !(value instanceof Value.RangeMatchableValue))
                                 .findAny()
                                 .isEmpty());
    }

    /**
     * Converts this {@link IndexComparison} into a corresponding protobuf message.
     * @return an equivalent protobuf message.
     */
    @Nonnull
    public abstract RecordMetaDataProto.Comparison toProto();

    /**
     * Converts this {@link IndexComparison} into an equivalent {@link Comparisons.Comparison}.
     * @return An equivalent {@link Comparisons.Comparison}.
     */
    @Nonnull
    public abstract Comparisons.Comparison toComparison();

    /**
     * A POJO equivalent for {@link com.apple.foundationdb.record.query.expressions.Comparisons.SimpleComparison}.
     */
    public static class SimpleComparison extends IndexComparison {

        /**
         * The type of the comparison, this is a subset of {@link com.apple.foundationdb.record.query.expressions.Comparisons.Type}.
         */
        public enum ComparisonType {
            EQUALS,
            NOT_EQUALS,
            LESS_THAN,
            LESS_THAN_OR_EQUALS,
            GREATER_THAN,
            GREATER_THAN_OR_EQUALS,
            STARTS_WITH,
            NOT_NULL,
            IS_NULL;
        }

        @Nonnull
        private final ComparisonType comparisonType;

        @Nonnull
        private final Object operand;

        @VisibleForTesting
        public SimpleComparison(final Comparisons.SimpleComparison comparison) {
            switch (comparison.getType()) {
                case EQUALS:
                    this.comparisonType = ComparisonType.EQUALS;
                    break;
                case NOT_EQUALS:
                    this.comparisonType = ComparisonType.NOT_EQUALS;
                    break;
                case LESS_THAN:
                    this.comparisonType = ComparisonType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUALS:
                    this.comparisonType = ComparisonType.LESS_THAN_OR_EQUALS;
                    break;
                case GREATER_THAN:
                    this.comparisonType = ComparisonType.GREATER_THAN;
                    break;
                case GREATER_THAN_OR_EQUALS:
                    this.comparisonType = ComparisonType.GREATER_THAN_OR_EQUALS;
                    break;
                case NOT_NULL:
                    this.comparisonType = ComparisonType.NOT_NULL;
                    break;
                case IS_NULL:
                    this.comparisonType = ComparisonType.IS_NULL;
                    break;
                default:
                    throw new RecordCoreException("attempt to construct PoJo index comparison from unsupported comparison type").addLogInfo(LogMessageKeys.COMPARISON_TYPE, comparison.getType());
            }
            this.operand = comparison.getComparand(null, null);
        }

        public SimpleComparison(@Nonnull final ComparisonType comparisonType, @Nonnull final Object operand) {
            this.comparisonType = comparisonType;
            this.operand = operand;
        }

        public SimpleComparison(@Nonnull final RecordMetaDataProto.SimpleComparison proto) {
            final var comparand = Objects.requireNonNull(LiteralKeyExpression.fromProtoValue(proto.getOperand()));
            ComparisonType comparisonType;
            switch (proto.getType()) {
                case EQUALS:
                    comparisonType = ComparisonType.EQUALS;
                    break;
                case NOT_EQUALS:
                    comparisonType = ComparisonType.NOT_EQUALS;
                    break;
                case LESS_THAN:
                    comparisonType = ComparisonType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUALS:
                    comparisonType = ComparisonType.LESS_THAN_OR_EQUALS;
                    break;
                case GREATER_THAN:
                    comparisonType = ComparisonType.GREATER_THAN;
                    break;
                case GREATER_THAN_OR_EQUALS:
                    comparisonType = ComparisonType.GREATER_THAN_OR_EQUALS;
                    break;
                case NOT_NULL:
                    comparisonType = ComparisonType.NOT_NULL;
                    break;
                case IS_NULL:
                    comparisonType = ComparisonType.IS_NULL;
                    break;
                default:
                    throw new RecordCoreException("attempt to deserialize unsupported comparison type").addLogInfo(LogMessageKeys.COMPARISON_TYPE, proto.getType());
            }
            this.comparisonType = comparisonType;
            this.operand = comparand;
        }

        @Nonnull
        public Object getOperand() {
            return operand;
        }

        @Nonnull
        public ComparisonType getComparisonType() {
            return comparisonType;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Comparison toProto() {
            RecordMetaDataProto.ComparisonType protoComparison;
            switch (comparisonType) {
                case EQUALS:
                    protoComparison = RecordMetaDataProto.ComparisonType.EQUALS;
                    break;
                case NOT_EQUALS:
                    protoComparison = RecordMetaDataProto.ComparisonType.NOT_EQUALS;
                    break;
                case LESS_THAN:
                    protoComparison = RecordMetaDataProto.ComparisonType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUALS:
                    protoComparison = RecordMetaDataProto.ComparisonType.LESS_THAN_OR_EQUALS;
                    break;
                case GREATER_THAN:
                    protoComparison = RecordMetaDataProto.ComparisonType.GREATER_THAN;
                    break;
                case GREATER_THAN_OR_EQUALS:
                    protoComparison = RecordMetaDataProto.ComparisonType.GREATER_THAN_OR_EQUALS;
                    break;
                case NOT_NULL:
                    protoComparison = RecordMetaDataProto.ComparisonType.NOT_NULL;
                    break;
                case IS_NULL:
                    protoComparison = RecordMetaDataProto.ComparisonType.IS_NULL;
                    break;
                default:
                    throw new RecordCoreException("serializing comparison type is not supported").addLogInfo(LogMessageKeys.COMPARISON_TYPE, comparisonType);
            }
            return RecordMetaDataProto.Comparison.newBuilder().setSimpleComparison(RecordMetaDataProto.SimpleComparison.newBuilder()
                            .setType(protoComparison)
                            .setOperand(LiteralKeyExpression.toProtoValue(operand))
                            .build())
                    .build();
        }

        @Nonnull
        @Override
        public Comparisons.Comparison toComparison() {
            Comparisons.Type type;
            switch (comparisonType) {
                case EQUALS:
                    type = Comparisons.Type.EQUALS;
                    break;
                case NOT_EQUALS:
                    type = Comparisons.Type.NOT_EQUALS;
                    break;
                case LESS_THAN:
                    type = Comparisons.Type.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUALS:
                    type = Comparisons.Type.LESS_THAN_OR_EQUALS;
                    break;
                case GREATER_THAN:
                    type = Comparisons.Type.GREATER_THAN;
                    break;
                case GREATER_THAN_OR_EQUALS:
                    type = Comparisons.Type.GREATER_THAN_OR_EQUALS;
                    break;
                case NOT_NULL:
                    type = Comparisons.Type.NOT_NULL;
                    break;
                case IS_NULL:
                    type = Comparisons.Type.IS_NULL;
                    break;
                default:
                    throw new RecordCoreException("serializing comparison type is not supported").addLogInfo(LogMessageKeys.COMPARISON_TYPE, comparisonType);
            }
            return new Comparisons.SimpleComparison(type, operand);
        }

        @Override
        public String toString() {
            return comparisonType.name() + ' ' + operand + ' ';
        }
    }

    /**
     * A POJO equivalent for {@link com.apple.foundationdb.record.query.expressions.Comparisons.NullComparison}.
     */
    public static class NullComparison extends IndexComparison {
        private final boolean isNull;

        public NullComparison(final boolean isNull) {
            this.isNull = isNull;
        }

        public NullComparison(@Nonnull final RecordMetaDataProto.NullComparison nullComparison) {
            this.isNull = nullComparison.getIsNull();
        }

        @VisibleForTesting
        public NullComparison(@Nonnull final Comparisons.NullComparison comparison) {
            this.isNull = comparison.getType().equals(Comparisons.Type.IS_NULL);
        }

        public boolean isNull() {
            return isNull;
        }

        @Nonnull
        @Override
        public RecordMetaDataProto.Comparison toProto() {
            return RecordMetaDataProto.Comparison.newBuilder()
                    .setNullComparison(RecordMetaDataProto.NullComparison.newBuilder().setIsNull(isNull).build()).build();
        }

        @Nonnull
        @Override
        public Comparisons.Comparison toComparison() {
            return new Comparisons.NullComparison(isNull ? Comparisons.Type.IS_NULL : Comparisons.Type.NOT_NULL);
        }

        @Override
        public String toString() {
            return isNull ? "IS NULL " : "IS NOT NULL ";
        }
    }
}
