/*
 * ValueMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VariadicFunctionValue;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AllOfMatcher.matchingAllOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate.typedMatcherWithPredicate;

/**
 * Matchers for descendants of {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ValueMatchers {
    private ValueMatchers() {
        // do not instantiate
    }

    public static BindingMatcher<Value> anyValue() {
        return typed(Value.class);
    }

    public static BindingMatcher<FieldValue> anyFieldValue() {
        return typed(FieldValue.class);
    }

    public static BindingMatcher<ConstantObjectValue> anyConstantObjectValue() {
        return typed(ConstantObjectValue.class);
    }

    public static BindingMatcher<PromoteValue> anyPromoteValue() {
        return typed(PromoteValue.class);
    }

    public static BindingMatcher<VariadicFunctionValue> anyVariadicFunction() {
        return typed(VariadicFunctionValue.class);
    }

    public static BindingMatcher<VariadicFunctionValue> variadicFunction(
            final VariadicFunctionValue.ComparisonFunction comparisonFunction) {
        return typedMatcherWithPredicate(VariadicFunctionValue.class,
                variadicFunctionValue -> variadicFunctionValue.getComparisonFunction() == comparisonFunction);
    }

    public static BindingMatcher<VariadicFunctionValue> coalesceFunction() {
        return variadicFunction(VariadicFunctionValue.ComparisonFunction.COALESCE);
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<LiteralValue<Boolean>> anyBooleanLiteralValue() {
        return typedMatcherWithPredicate((Class<LiteralValue<Boolean>>)(Class<?>)LiteralValue.class,
                t -> t.getResultType().getTypeCode() == Type.TypeCode.BOOLEAN);
    }

    public static BindingMatcher<NullValue> nullValue() {
        return typed(NullValue.class);
    }

    public static BindingMatcher<Value> anyNotNullableValue() {
        return typedMatcherWithPredicate(Value.class, value -> !value.getResultType().isNullable());
    }

    public static <V extends Value> BindingMatcher<FieldValue> fieldValue(final BindingMatcher<V> downstreamValueMatcher) {
        return typedWithDownstream(FieldValue.class,
                Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValueMatcher);
    }

    public static BindingMatcher<FieldValue> fieldValueWithFieldNames(final String fieldPathAsString) {
        return fieldValueWithFieldNames(anyValue(), fieldPathAsString);
    }

    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldNames(final BindingMatcher<V> downstreamValue,
                                                                                        final String fieldPathAsString) {
        final ImmutableList<BindingMatcher<String>> fieldPathMatchers =
                Arrays.stream(fieldPathAsString.split("\\."))
                        .map(PrimitiveMatchers::equalsObject)
                        .collect(ImmutableList.toImmutableList());
        return fieldValueWithFieldNames(downstreamValue, exactly(fieldPathMatchers));
    }

    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldNames(final BindingMatcher<V> downstreamValue,
                                                                                        final CollectionMatcher<String> downstreamFieldPath) {
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamValueMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getFieldPathNames, name -> "fieldPathNames(" + name + ")"),
                        downstreamFieldPath);

        return typedWithDownstream(FieldValue.class,
                Extractor.identity(),
                matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathMatcher)));
    }

    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithLastFieldName(final BindingMatcher<V> downstreamValue,
                                                                                           final BindingMatcher<String> downstreamFieldNameMatcher) {
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamValueMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(fieldValue -> {
                            final var fieldPathNames = fieldValue.getFieldPathNames();
                            return fieldPathNames.get(fieldPathNames.size() - 1);
                        }, name -> "fieldPathNames(" + name + ")"),
                        downstreamFieldNameMatcher);

        return typedWithDownstream(FieldValue.class,
                Extractor.identity(),
                matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathMatcher)));
    }

    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldPath(final BindingMatcher<V> downstreamValue,
                                                                                       final CollectionMatcher<Integer> downstreamFieldPathOrdinals,
                                                                                       final CollectionMatcher<Type> downstreamFieldPathTypes) {
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamValueMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathOrdinalsMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(f -> f.getFieldOrdinals().asList(), name -> "fieldPathOrdinals(" + name + ")"),
                        downstreamFieldPathOrdinals);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathTypesMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getFieldPathTypes, name -> "fieldPathTypes(" + name + ")"),
                        downstreamFieldPathTypes);

        return typedWithDownstream(FieldValue.class,
                Extractor.identity(),
                matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathOrdinalsMatcher, downstreamFieldPathTypesMatcher)));
    }

    public static BindingMatcher<NumericAggregationValue.Sum> sumAggregationValue() {
        return sumAggregationValue(anyValue());
    }

    public static <V extends Value> BindingMatcher<NumericAggregationValue.Sum> sumAggregationValue(final BindingMatcher<V> downstream) {
        return typedWithDownstream(NumericAggregationValue.Sum.class,
                Extractor.of(NumericAggregationValue.Sum::getChild, name -> "child(" + name + ")"),
                downstream);
    }

    public static <V extends Value> BindingMatcher<NumericAggregationValue.BitmapConstructAgg> bitmapConstructAggValue(final BindingMatcher<V> downstream) {
        return typedWithDownstream(NumericAggregationValue.BitmapConstructAgg.class,
                Extractor.of(NumericAggregationValue.BitmapConstructAgg::getChild, name -> "child(" + name + ")"),
                downstream);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordConstructorValue> recordConstructorValue(final BindingMatcher<? extends Value>... downstreamValues) {
        return recordConstructorValue(exactly(Arrays.asList(downstreamValues)));
    }

    public static BindingMatcher<RecordConstructorValue> recordConstructorValue(final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(RecordConstructorValue.class,
                Extractor.of(RecordConstructorValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    public static BindingMatcher<ArithmeticValue> arithmeticValue(final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(ArithmeticValue.class,
                Extractor.of(ArithmeticValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    public static BindingMatcher<StreamableAggregateValue> streamableAggregateValue() {
        return streamableAggregateValue(exactly(ImmutableList.of(anyValue())));
    }

    public static BindingMatcher<StreamableAggregateValue> streamableAggregateValue(final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(StreamableAggregateValue.class,
                Extractor.of(StreamableAggregateValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    public static BindingMatcher<QuantifiedObjectValue> quantifiedObjectValue() {
        return typed(QuantifiedObjectValue.class);
    }

    public static BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(ToOrderedBytesValue.class,
                Extractor.of(ToOrderedBytesValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    public static <V extends Value> BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(final BindingMatcher<V> downstreamValueMatcher) {
        return typedWithDownstream(ToOrderedBytesValue.class,
                Extractor.of(ToOrderedBytesValue::getChild, name -> "child(" + name + ")"),
                downstreamValueMatcher);
    }

    public static <V extends Value> BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(final BindingMatcher<V> downstreamValue,
                                                                                            final TupleOrdering.Direction direction) {
        final TypedMatcherWithExtractAndDownstream<ToOrderedBytesValue> downstreamValueMatcher =
                typedWithDownstream(ToOrderedBytesValue.class,
                        Extractor.of(ToOrderedBytesValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<ToOrderedBytesValue> downstreamDirectionMatcher =
                typedWithDownstream(ToOrderedBytesValue.class,
                        Extractor.of(ToOrderedBytesValue::getDirection, name -> "direction(" + name + ")"),
                        PrimitiveMatchers.equalsObject(direction));

        return typedWithDownstream(ToOrderedBytesValue.class,
                Extractor.identity(),
                matchingAllOf(ToOrderedBytesValue.class, ImmutableList.of(downstreamValueMatcher, downstreamDirectionMatcher)));
    }
}
