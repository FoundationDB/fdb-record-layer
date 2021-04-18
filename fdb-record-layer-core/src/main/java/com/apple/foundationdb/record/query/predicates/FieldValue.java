/*
 * FieldValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.MessageValue;
import com.apple.foundationdb.record.query.plan.temp.matchers.AllOfMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.Extractor;
import com.apple.foundationdb.record.query.plan.temp.matchers.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * A value representing the contents of a (non-repeated, arbitrarily-nested) field of a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final QuantifiedColumnValue columnValue;
    @Nonnull
    private final List<String> fieldPath;

    public FieldValue(@Nonnull QuantifiedColumnValue columnValue, @Nonnull List<String> fieldPath) {
        Preconditions.checkArgument(!fieldPath.isEmpty());
        this.columnValue = columnValue;
        this.fieldPath = ImmutableList.copyOf(fieldPath);
    }

    @Nonnull
    public List<String> getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    public List<String> getFieldPrefix() {
        return fieldPath.subList(0, fieldPath.size() - 1);
    }

    @Nonnull
    public String getFieldName() {
        return fieldPath.get(fieldPath.size() - 1);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return columnValue;
    }

    @Nonnull
    @Override
    public FieldValue withNewChild(@Nonnull final Value child) {
        return new FieldValue((QuantifiedColumnValue)child, fieldPath);
    }

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        if (message == null) {
            return null;
        }
        final Object childResult = columnValue.eval(store, context, record, message);
        if (!(childResult instanceof Message)) {
            return null;
        }
        return MessageValue.getFieldValue((Message)childResult, fieldPath);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!ValueWithChild.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        final FieldValue that = (FieldValue)other;
        return columnValue.semanticEquals(that.columnValue, equivalenceMap) &&
               fieldPath.equals(that.fieldPath);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldPath);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, fieldPath);
    }

    @Override
    public String toString() {
        return columnValue.toString() + "/" + String.join(".", fieldPath);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(columnValue.getCorrelatedTo()));
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
