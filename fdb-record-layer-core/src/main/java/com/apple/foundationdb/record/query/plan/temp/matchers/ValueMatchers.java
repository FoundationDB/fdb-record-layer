/*
 * ValueMatchers.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import java.util.Arrays;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for descendants of {@link com.apple.foundationdb.record.query.predicates.Value}.
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
    public static <V extends Value> BindingMatcher<FieldValue> fieldValue(@Nonnull final BindingMatcher<V> downstreamValue,
                                                                          @Nonnull final String fieldPathAsString) {
        final ImmutableList<BindingMatcher<String>> fieldPathMatchers =
                Arrays.stream(fieldPathAsString.split("\\."))
                        .map(PrimitiveMatchers::equalsObject)
                        .collect(ImmutableList.toImmutableList());
        return fieldValue(downstreamValue, exactly(fieldPathMatchers));
    }

    @Nonnull
    public static <V1 extends Value> BindingMatcher<FieldValue> fieldValue(@Nonnull final BindingMatcher<V1> downstreamValue,
                                                                           @Nonnull final CollectionMatcher<String> downstreamFieldPath) {
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamValueMatcher =
                typedWithDownstream(FieldValue.class, FieldValue::getChild, downstreamValue);
        final TypedMatcherWithExtractAndDownstream<FieldValue> downstreamFieldPathMatcher =
                typedWithDownstream(FieldValue.class, FieldValue::getFieldPath, downstreamFieldPath);
        return typedWithDownstream(FieldValue.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(FieldValue.class, ImmutableList.of(downstreamValueMatcher, downstreamFieldPathMatcher)));
    }
}
