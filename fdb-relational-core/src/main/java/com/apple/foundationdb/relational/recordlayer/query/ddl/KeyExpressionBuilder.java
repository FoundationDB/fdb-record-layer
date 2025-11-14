/*
 * KeyExpressionBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueVisitorWithDefaults;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public final class KeyExpressionBuilder {

    @Nonnull
    private final ValueToKeyExpressionVisitor.KeyExpressionConstruction keyExpressionConstruction;

    private KeyExpressionBuilder(@Nonnull final Value value, @Nonnull final KeyExpressionDecorator keyExpressionDecorator) {
        final var visitor = new ValueToKeyExpressionVisitor(keyExpressionDecorator);
        keyExpressionConstruction = visitor.visit(value);
    }

    @Nonnull
    public KeyExpression getKeyExpression() {
        return Assert.notNullUnchecked(keyExpressionConstruction.getKeyExpression());
    }

    @Nonnull
    public String getIndexType() {
        @Nullable final var indexType = keyExpressionConstruction.getIndexType();
        return indexType == null ? IndexTypes.VALUE : indexType;
    }

    @Nonnull
    public Type getBaseType() {
        @Nullable final var baseType = keyExpressionConstruction.getBaseType();
        return baseType == null ? Type.any() : baseType;
    }

    @Nonnull
    public String getBaseTypeName() {
        final var type = getBaseType();
        Assert.thatUnchecked(type.isRecord());
        final var relationType = Assert.castUnchecked(type, Type.Record.class);
        return Assert.notNullUnchecked(relationType.getName());
    }

    private static final class ValueToKeyExpressionVisitor implements ValueVisitorWithDefaults<ValueToKeyExpressionVisitor.KeyExpressionConstruction> {

        @Nonnull
        private final KeyExpressionDecorator keyExpressionDecorator;

        ValueToKeyExpressionVisitor(@Nonnull final KeyExpressionDecorator keyExpressionDecorator) {
            this.keyExpressionDecorator = keyExpressionDecorator;
        }

        @Nonnull
        private KeyExpressionConstruction decorate(@Nonnull final Value value, @Nonnull final KeyExpressionConstruction keyExpressionConstruction) {
            final var keyExpression = keyExpressionConstruction.keyExpression;
            if (keyExpression == null) {
                return keyExpressionConstruction;
            }
            return keyExpressionConstruction.replaceKeyExpression(Objects.requireNonNull(keyExpressionDecorator.decorate(value, keyExpression)));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitRecordConstructorValue(@Nonnull final RecordConstructorValue recordConstructorValue) {
            final var fieldExpressionsBuilder = ImmutableList.<KeyExpressionConstruction>builder();
            for (final var fieldValue : recordConstructorValue.getChildren()) {
                fieldExpressionsBuilder.add(visit(fieldValue));
            }
            final var fieldExpressions = fieldExpressionsBuilder.build();
            return decorate(recordConstructorValue, KeyExpressionConstruction.combine(fieldExpressions));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitDefault(@Nonnull final Value element) {
            throw new RecordCoreException("generating key expression from " + element + " is not supported");
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitSum(@Nonnull final NumericAggregationValue.Sum element) {
            final var child = visit(element.getChild());
            return decorate(element, child.withIndexType(IndexTypes.SUM));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitCountValue(@Nonnull final CountValue element) {
            final var child = visit(Iterables.getOnlyElement(element.getChildren()));
            return decorate(element, child.withIndexType(IndexTypes.COUNT));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitMaxEverValue(@Nonnull final IndexOnlyAggregateValue.MaxEverValue element) {
            final var child = visit(element.getChild());
            if (keyExpressionDecorator.useLegacyExtremum) {
                Verify.verify(element.getChild().getResultType().isNumeric(), "only numeric types allowed in " +
                        IndexTypes.MAX_EVER_LONG + " aggregation operation");
                return decorate(element, child.withIndexType(IndexTypes.MAX_EVER_LONG));
            }
            return decorate(element, child.withIndexType(IndexTypes.MAX_EVER_TUPLE));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitMinEverValue(@Nonnull final IndexOnlyAggregateValue.MinEverValue element) {
            final var child = visit(element.getChild());
            if (keyExpressionDecorator.useLegacyExtremum) {
                Verify.verify(element.getChild().getResultType().isNumeric(), "only numeric types allowed in " +
                        IndexTypes.MIN_EVER_LONG + " aggregation operation");
                return decorate(element, child.withIndexType(IndexTypes.MIN_EVER_LONG));
            }
            return decorate(element, child.withIndexType(IndexTypes.MIN_EVER_TUPLE));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitFieldValue(@Nonnull final FieldValue fieldValue) {
            final var keyExpression = toKeyExpression(fieldValue);
            final var keyExpressionConstruction = visit(fieldValue.getChild());
            return decorate(fieldValue, keyExpressionConstruction.withKeyExpression(keyExpression));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitQuantifiedObjectValue(@Nonnull final QuantifiedObjectValue quantifiedObjectValue) {
            return decorate(quantifiedObjectValue, KeyExpressionConstruction.ofBaseType(quantifiedObjectValue.getResultType()));
        }

        @Nonnull
        @Override
        public KeyExpressionConstruction visitQueriedValue(@Nonnull final QueriedValue queriedValue) {
            return decorate(queriedValue, KeyExpressionConstruction.ofBaseType(queriedValue.getResultType()));
        }

        @Nonnull
        private static KeyExpression toKeyExpression(@Nonnull final FieldValue fieldValue) {
            return toKeyExpression(fieldValue, 0);
        }

        @Nonnull
        private static KeyExpression toKeyExpression(@Nonnull final FieldValue fieldValue, int index) {
            final var field = fieldValue.getFieldPath().getFieldAccessors().get(index);
            final var keyExpression = toKeyExpression(Assert.notNullUnchecked(field.getName()), field.getType());
            if (index + 1 < fieldValue.getFieldPath().getFieldAccessors().size()) {
                return keyExpression.nest(toKeyExpression(fieldValue, index + 1));
            }
            return keyExpression;
        }

        @Nonnull
        public static FieldKeyExpression toKeyExpression(@Nonnull final String name, @Nonnull final Type type) {
            Assert.notNullUnchecked(name);
            final var fanType = type.getTypeCode() == Type.TypeCode.ARRAY ?
                                KeyExpression.FanType.FanOut :
                                KeyExpression.FanType.None;
            return field(name, fanType);
        }

        public static final class KeyExpressionConstruction {
            @Nullable
            private final KeyExpression keyExpression;

            @Nullable
            private final Type baseType;

            @Nullable
            private final String indexType;

            private KeyExpressionConstruction(@Nullable final KeyExpression keyExpression, @Nullable final Type baseType, @Nullable String indexType) {
                this.keyExpression = keyExpression;
                this.baseType = baseType;
                this.indexType = indexType;
            }

            @Nonnull
            public KeyExpressionConstruction withKeyExpression(@Nonnull final KeyExpression keyExpression) {
                Assert.isNullUnchecked(this.keyExpression);
                return new KeyExpressionConstruction(keyExpression, baseType, indexType);
            }

            @Nonnull
            KeyExpressionConstruction replaceKeyExpression(@Nonnull final KeyExpression keyExpression) {
                return new KeyExpressionConstruction(keyExpression, baseType, indexType);
            }

            @Nonnull
            public KeyExpressionConstruction withBaseType(@Nonnull final Type type) {
                if (baseType == null) {
                    return new KeyExpressionConstruction(keyExpression, type, indexType);
                }
                Assert.thatUnchecked(baseType.equals(type), "defining key expression on multiple types is not supported");
                return this;
            }

            @Nonnull
            public KeyExpressionConstruction withIndexType(@Nonnull final String indexType) {
                if (this.indexType == null) {
                    return new KeyExpressionConstruction(keyExpression, baseType, indexType);
                }
                Assert.thatUnchecked(this.indexType.equals(indexType), "defining a key expression with multiple index types is not supported");
                return this;
            }

            @Nullable
            public Type getBaseType() {
                return baseType;
            }

            @Nullable
            public String getIndexType() {
                return indexType;
            }

            @Nullable
            public KeyExpression getKeyExpression() {
                return keyExpression;
            }

            private boolean isAggregate() {
                return IndexTypes.COUNT.equals(indexType) ||
                        IndexTypes.COUNT_NOT_NULL.equals(indexType) ||
                        IndexTypes.SUM.equals(indexType) ||
                        IndexTypes.MAX_EVER_LONG.equals(indexType) ||
                        IndexTypes.PERMUTED_MAX.equals(indexType) ||
                        IndexTypes.MIN_EVER_LONG.equals(indexType) ||
                        IndexTypes.MIN_EVER_TUPLE.equals(indexType) ||
                        IndexTypes.BITMAP_VALUE.equals(indexType);
            }

            @Nonnull
            static KeyExpressionConstruction ofBaseType(@Nonnull final Type baseType) {
                return new KeyExpressionConstruction(null, baseType, null);
            }

            @Nonnull
            static KeyExpressionConstruction ofExpression(@Nonnull final KeyExpression expression) {
                return new KeyExpressionConstruction(expression, null, null);
            }

            @Nonnull
            static KeyExpressionConstruction ofIndexType(@Nonnull final String indexType) {
                return new KeyExpressionConstruction(null, null, indexType);
            }

            @Nonnull
            static KeyExpressionConstruction combine(@Nonnull final List<KeyExpressionConstruction> fields) {
                Assert.thatUnchecked(!fields.isEmpty());
                if (fields.size() == 1) {
                    return fields.get(0);
                }
                final var fieldTypes = fields.stream().map(KeyExpressionConstruction::getBaseType).flatMap(Stream::ofNullable).collect(ImmutableSet.toImmutableSet());
                Assert.thatUnchecked(fieldTypes.size() == 1, "defining key expression on multiple base types is not supported");
                final var indexTypes = fields.stream().map(KeyExpressionConstruction::getIndexType).flatMap(Stream::ofNullable).collect(ImmutableSet.toImmutableSet());
                Assert.thatUnchecked(indexTypes.size() <= 1, "defining key expression on multiple index types is not supported");
                final var fieldKeyExpressions = fields.stream().map(KeyExpressionConstruction::getKeyExpression).flatMap(Stream::ofNullable).collect(ImmutableList.toImmutableList());
                final var containsAggregates = fields.stream().anyMatch(KeyExpressionConstruction::isAggregate);
                if (!containsAggregates) {
                    return new KeyExpressionConstruction(Key.Expressions.concat(fieldKeyExpressions), Assert.optionalUnchecked(fieldTypes.stream().findFirst()), null);
                }
                final var aggregateKeyExpression = generateAggregateIndexKeyExpression(fields);
                return aggregateKeyExpression.withBaseType(Assert.optionalUnchecked(fieldTypes.stream().findFirst()));
            }

            @Nonnull
            private static KeyExpressionConstruction generateAggregateIndexKeyExpression(@Nonnull List<KeyExpressionConstruction> fields) {
                final var remainingFieldsBuilder = ImmutableList.<KeyExpressionConstruction>builder();
                KeyExpressionConstruction aggregateField = null;
                for (final var field : fields) {
                    if (field.isAggregate()) {
                        Assert.isNullUnchecked(aggregateField, "defining key expression with multiple aggregations is not supported");
                        aggregateField = field;
                    } else {
                        remainingFieldsBuilder.add(field);
                    }
                }
                Assert.notNullUnchecked(aggregateField);
                final var remainingFields = remainingFieldsBuilder.build();
                var indexTypeName = Assert.notNullUnchecked(aggregateField.indexType);
                final KeyExpression groupedValue;
                final GroupingKeyExpression keyExpression;
                // COUNT(*) is a special case.
                if (IndexTypes.COUNT.equals(indexTypeName)) {
                    if (!remainingFields.isEmpty()) {
                        keyExpression = new GroupingKeyExpression(Assert.notNullUnchecked(combine(remainingFields).keyExpression), 0);
                    } else {
                        keyExpression = new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0);
                    }
//            } else if (aggregateValue instanceof NumericAggregationValue.BitmapConstructAgg && IndexTypes.BITMAP_VALUE.equals(indexTypeName)) {
//                Assert.thatUnchecked(child instanceof FieldValue || child instanceof ArithmeticValue, "Unsupported index definition, expecting a column argument in aggregation function");
//                groupedValue = generate(List.of(child), Collections.emptyMap());
//                // only support bitmap_construct_agg(bitmap_bit_position(column))
//                // doesn't support bitmap_construct_agg(column)
//                Assert.thatUnchecked(groupedValue instanceof FunctionKeyExpression, "Unsupported index definition, expecting a bitmap_bit_position function in bitmap_construct_agg function");
//                final FunctionKeyExpression functionGroupedValue = (FunctionKeyExpression) groupedValue;
//                Assert.thatUnchecked(BITMAP_BIT_POSITION.equals(functionGroupedValue.getName()), "Unsupported index definition, expecting a bitmap_bit_position function in bitmap_construct_agg function");
//                final var groupedColumnValue = ((ThenKeyExpression) ((FunctionKeyExpression) groupedValue).getArguments()).getChildren().get(0);
//
//                if (maybeGroupingExpression.isPresent()) {
//                    final var afterRemove = removeBitmapBucketOffset(maybeGroupingExpression.get());
//                    if (afterRemove == null) {
//                        keyExpression = ((FieldKeyExpression) groupedColumnValue).ungrouped();
//                    } else {
//                        keyExpression = ((FieldKeyExpression) groupedColumnValue).groupBy(afterRemove);
//                    }
//                } else {
//                    throw Assert.failUnchecked("Unsupported index definition, unexpected grouping expression " + groupedValue);
//                }
                } else {
                    groupedValue = aggregateField.keyExpression;
                    Assert.thatUnchecked(groupedValue instanceof FieldKeyExpression || groupedValue instanceof ThenKeyExpression);
                    if (!remainingFields.isEmpty()) {
                        keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                                        ((FieldKeyExpression)groupedValue).groupBy(Assert.notNullUnchecked(combine(remainingFields).keyExpression)) :
                                        ((ThenKeyExpression)groupedValue).groupBy(Assert.notNullUnchecked(combine(remainingFields).keyExpression));
                    } else {
                        keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                                        ((FieldKeyExpression)groupedValue).ungrouped() :
                                        ((ThenKeyExpression)groupedValue).ungrouped();
                    }
                }
                return KeyExpressionConstruction.ofExpression(keyExpression).withIndexType(aggregateField.getIndexType());
            }
        }
    }

    public static final class KeyExpressionDecorator {
        @Nonnull
        private final SetMultimap<Value, Function<KeyExpression, KeyExpression>> expressionDecorationMap;

        private final boolean useLegacyExtremum;

        private KeyExpressionDecorator(@Nonnull final SetMultimap<Value, Function<KeyExpression, KeyExpression>> expressionDecorationMap,
                                       final boolean useLegacyExtremum) {
            this.expressionDecorationMap = expressionDecorationMap;
            this.useLegacyExtremum = useLegacyExtremum;
        }

        @Nullable
        KeyExpression decorate(@Nonnull final Value value, @Nullable final KeyExpression keyExpression) {
            if (keyExpression == null) {
                return null;
            }
            return expressionDecorationMap.get(value).stream()
                    .reduce(keyExpression, (acc, function) -> Objects.requireNonNull(function).apply(acc), (a, b) -> b);
        }

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        public static final class Builder {

            @Nonnull
            private final SetMultimap<Value, Function<KeyExpression, KeyExpression>> expressionDecorationMap;

            private boolean useLegacyExtremum;

            Builder() {
                expressionDecorationMap = Multimaps.newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);
            }

            @Nonnull
            public Builder addOrderKeyExpression(@Nonnull final Value value, @Nonnull final ColumnSort columnSort) {
                expressionDecorationMap.put(value, keyExpression -> Key.Expressions.function(columnSort.getKeyExpressionFunctionName(), keyExpression));
                return this;
            }

            @Nonnull
            public Builder addKeyValueExpression(@Nonnull final Value value, int keySize) {
                expressionDecorationMap.put(value, keyExpression -> Key.Expressions.keyWithValue(keyExpression, keySize));
                return this;
            }

            @Nonnull
            public Builder setUseLegacyExtremum(boolean useLegacyExtremum) {
                this.useLegacyExtremum = useLegacyExtremum;
                return this;
            }

            @Nonnull
            public KeyExpressionDecorator build() {
                return new KeyExpressionDecorator(expressionDecorationMap, useLegacyExtremum);
            }
        }
    }

    @Nonnull
    public static KeyExpressionBuilder buildKeyExpression(@Nonnull final Value value, @Nonnull final KeyExpressionDecorator keyExpressionDecorator) {
        return new KeyExpressionBuilder(value, keyExpressionDecorator);
    }
}
