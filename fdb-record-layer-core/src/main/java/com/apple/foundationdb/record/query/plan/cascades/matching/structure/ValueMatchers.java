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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;

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
                AllOfMatcher.matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathMatcher)));
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<FieldValue> fieldValueWithFieldPath(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                       @Nonnull final CollectionMatcher<Type.Record.Field> downstreamFieldPath) {
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamValueMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathMatcher =
                typedWithDownstream(FieldValue.class,
                        Extractor.of(FieldValue::getFields, name -> "fieldPath(" + name + ")"),
                        downstreamFieldPath);

        return typedWithDownstream(FieldValue.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathMatcher)));
    }

    @Nonnull
    public static BindingMatcher<NumericAggregationValue> numericAggregationValue(@Nonnull final String operatorName) {
        return numericAggregationValue(anyValue(), operatorName);
    }

    @Nonnull
    public static <V extends Value> BindingMatcher<NumericAggregationValue> numericAggregationValue(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                                                    @Nonnull final String operatorName) {
        final TypedMatcherWithExtractAndDownstream<NumericAggregationValue> downstreamValueMatcher =
                typedWithDownstream(NumericAggregationValue.class,
                        Extractor.of(NumericAggregationValue::getChild, name -> "child(" + name + ")"),
                        downstreamValue);
        final TypedMatcherWithExtractAndDownstream<NumericAggregationValue> downstreamOperatorMatcher =
                typedWithDownstream(NumericAggregationValue.class,
                        Extractor.of(NumericAggregationValue::getOperatorName, name -> "operator(" + name + ")"),
                        PrimitiveMatchers.equalsObject(operatorName));
        return typedWithDownstream(NumericAggregationValue.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(NumericAggregationValue.class, ImmutableList.of(downstreamValueMatcher, downstreamOperatorMatcher)));
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
    public static BindingMatcher<QuantifiedObjectValue> currentObjectValue() {
        return typedWithDownstream(QuantifiedObjectValue.class,
                Extractor.of(QuantifiedObjectValue::getAlias, name -> "alias(" + name + ")"),
                PrimitiveMatchers.equalsObject(Quantifier.CURRENT));
    }

    @Nonnull
    public static BindingMatcher<StreamableAggregateValue> streamableAggregateValue() {
        return typed(StreamableAggregateValue.class);
    }
}
