/*
 * EmptyComparison.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A comparison to determine whether a repeated field is empty (has no occurrences).
 */
@API(API.Status.MAINTAINED)
public class EmptyComparison extends BaseRepeatedField implements ComponentWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Empty-Comparison");

    private final boolean isEmpty;

    public EmptyComparison(@Nonnull String fieldName, boolean isEmpty) {
        super(fieldName, Field.OneOfThemEmptyMode.EMPTY_NO_MATCHES);
        this.isEmpty = isEmpty;
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null) {
            return null;
        }
        final int count = message.getRepeatedFieldCount(findFieldDescriptor(message));
        if (isEmpty) {
            return (count == 0);
        } else {
            return (count > 0);
        }
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        validateRepeatedField(descriptor);
    }

    @Override
    public String toString() {
        return getFieldName() + (isEmpty ? " IS_EMPTY" : " IS_NOT_EMPTY");
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    @Override
    public ExpandedPredicates normalizeForPlanner(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(getFieldName())
                .build();
        return ExpandedPredicates.ofPredicate(new ValuePredicate(new FieldValue(baseAlias, fieldNames), Comparisons.LIST_EMPTY));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EmptyComparison that = (EmptyComparison) o;
        return isEmpty == that.isEmpty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isEmpty);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return isEmpty ? 1 : 0;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, isEmpty);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }
}
