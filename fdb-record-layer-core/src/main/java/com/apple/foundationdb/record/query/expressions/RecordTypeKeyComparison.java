/*
 * RecordTypeKeyComparison.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link QueryComponent} that implements checking for a given record type.
 * Not normally used in queries.
 */
@API(API.Status.MAINTAINED)
public class RecordTypeKeyComparison implements ComponentWithComparison {
    @Nonnull
    private final RecordTypeComparison comparison;

    public RecordTypeKeyComparison(@Nonnull String recordTypeName) {
        this.comparison = new RecordTypeComparison(recordTypeName);
    }

    public static boolean hasRecordTypeKeyComparison(@Nonnull ScanComparisons scanComparisons) {
        return scanComparisons.getEqualitySize() > 0 && scanComparisons.getEqualityComparisons().get(0) instanceof RecordTypeComparison;
    }

    public static Set<String> recordTypeKeyComparisonTypes(@Nonnull ScanComparisons scanComparisons) {
        return Collections.singleton(((RecordTypeComparison)scanComparisons.getEqualityComparisons().get(0)).recordTypeName);
    }

    @Override
    public String getName() {
        return getComparison().typelessString();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        return getComparison().eval(store, context, message);
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        // Usable against any record type.
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return getComparison().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RecordTypeKeyComparison that = (RecordTypeKeyComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash() {
        return getComparison().planHash();
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        throw new UnsupportedOperationException("Cannot change comparison");
    }

    static class RecordTypeComparison implements Comparisons.Comparison {
        private final String recordTypeName;

        RecordTypeComparison(String recordTypeName) {
            this.recordTypeName = recordTypeName;
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            if (value == null) {
                return null;
            }
            return ((Message)value).getDescriptorForType().getName().equals(recordTypeName);
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // Do not actually apply to any particular field.
        }

        @Nonnull
        @Override
        public Comparisons.Type getType() {
            return Comparisons.Type.EQUALS;
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (store == null) {
                throw new Comparisons.EvaluationContextRequiredException("Cannot get record type key without store");
            }
            return store.getRecordMetaData().getRecordType(recordTypeName).getRecordTypeKey();
        }

        @Nonnull
        @Override
        public String typelessString() {
            return recordTypeName;
        }

        @Override
        public int planHash() {
            return PlanHashable.objectPlanHash(recordTypeName);
        }

        @Override
        public String toString() {
            return "IS " + recordTypeName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RecordTypeComparison that = (RecordTypeComparison)o;
            return Objects.equals(recordTypeName, that.recordTypeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordTypeName);
        }
    }
}
