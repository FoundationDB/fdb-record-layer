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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AllOfMatcher.matchingAllOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for descendants of {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
public class ValueMatchers {
    private ValueMatchers() {
        // do not instantiate
    }

    @Nonnull
    public static BindingMatcher<Value> anyValue() {
        return typed(Value.class);
    }

    @Nonnull
    public static BindingMatcher<FieldValue> anyFieldValue() {
        return typed(FieldValue.class);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValue(@Nonnull final BindingMatcher<V> downstreamValueMatcher) {
        return typedWithDownstream(FieldValue.class,
                Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValueMatcher);
    }

    @Nonnull
    public static BindingMatcher<FieldValue> fieldValueWithFieldNames(@Nonnull final String fieldPathAsString) {
        return fieldValueWithFieldNames(anyValue(), fieldPathAsString);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldNames(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                        @Nonnull final String fieldPathAsString) {
        final ImmutableList<BindingMatcher<String>> fieldPathMatchers =
                Arrays.stream(fieldPathAsString.split("\\."))
                        .map(PrimitiveMatchers::equalsObject)
                        .collect(ImmutableList.toImmutableList());
        return fieldValueWithFieldNames(downstreamValue, exactly(fieldPathMatchers));
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldNames(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                        @Nonnull final CollectionMatcher<String> downstreamFieldPath) {
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

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithLastFieldName(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                           @Nonnull final BindingMatcher<String> downstreamFieldNameMatcher) {
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

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldPath(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                       @Nonnull final CollectionMatcher<Integer> downstreamFieldPathOrdinals,
                                                                                       @Nonnull final CollectionMatcher<Type> downstreamFieldPathTypes) {
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

    @Nonnull
    public static <V extends Value> BindingMatcher<NumericAggregationValue.Sum> sumAggregationValue(@Nonnull final BindingMatcher<V> downstream) {
        return typedWithDownstream(NumericAggregationValue.Sum.class,
                Extractor.of(NumericAggregationValue.Sum::getChild, name -> "child(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<NumericAggregationValue.BitmapConstructAgg> bitmapConstructAggValue(@Nonnull final BindingMatcher<V> downstream) {
        return typedWithDownstream(NumericAggregationValue.BitmapConstructAgg.class,
                Extractor.of(NumericAggregationValue.BitmapConstructAgg::getChild, name -> "child(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static BindingMatcher<RecordConstructorValue> recordConstructorValue(@Nonnull final BindingMatcher<? extends Value>... downstreamValues) {
        return recordConstructorValue(exactly(Arrays.asList(downstreamValues)));
    }

    @Nonnull
    public static BindingMatcher<RecordConstructorValue> recordConstructorValue(@Nonnull final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(RecordConstructorValue.class,
                Extractor.of(RecordConstructorValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    @Nonnull
    public static BindingMatcher<ArithmeticValue> arithmeticValue(@Nonnull final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(ArithmeticValue.class,
                Extractor.of(ArithmeticValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    @Nonnull
    public static BindingMatcher<StreamableAggregateValue> streamableAggregateValue() {
        return streamableAggregateValue(exactly(ImmutableList.of(anyValue())));
    }

    @Nonnull
    public static BindingMatcher<StreamableAggregateValue> streamableAggregateValue(@Nonnull final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(StreamableAggregateValue.class,
                Extractor.of(StreamableAggregateValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    @Nonnull
    public static BindingMatcher<VersionValue> versionValue() {
        return typed(VersionValue.class);
    }

    @Nonnull
    public static BindingMatcher<QuantifiedObjectValue> quantifiedObjectValue() {
        return typed(QuantifiedObjectValue.class);
    }

    @Nonnull
    public static BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(@Nonnull final CollectionMatcher<? extends Value> downstreamValues) {
        return typedWithDownstream(ToOrderedBytesValue.class,
                Extractor.of(ToOrderedBytesValue::getChildren, name -> "children(" + name + ")"),
                downstreamValues);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(@Nonnull final BindingMatcher<V> downstreamValueMatcher) {
        return typedWithDownstream(ToOrderedBytesValue.class,
                Extractor.of(ToOrderedBytesValue::getChild, name -> "child(" + name + ")"),
                downstreamValueMatcher);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<ToOrderedBytesValue> toOrderedBytesValue(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                            @Nonnull final TupleOrdering.Direction direction) {
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
