/*
 * RecordQuerySortKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQuerySortKey;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.sorting.MemorySortAdapter;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Defines, logically, how {@link RecordQuerySortPlan} should sort the records.
 * Also acts as a factory for {@link RecordQuerySortAdapter}.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQuerySortKey implements PlanHashable, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Sort-Key");

    @Nonnull
    private final KeyExpression key;
    private final boolean reverse;

    public RecordQuerySortKey(@Nonnull final KeyExpression key, final boolean reverse) {
        this.key = key;
        this.reverse = reverse;
    }

    @Nonnull
    public KeyExpression getKey() {
        return key;
    }

    public boolean isReverse() {
        return reverse;
    }

    /**
     * Get a sort adapter used for a single invocation of a plan using this key.
     * The adapter will have a unique encryption key.
     * The limit on the number of records returned limits the size of the sort buffer that needs to be kept,
     * and so may allow it to be in-memory only.
     * @param <M> type used to represent stored records
     * @param recordStore the record store against which the plan is running
     * @param maxRecordsToRead the maximum number of records to read
     * @return a new sort adapter
     */
    @Nonnull
    public <M extends Message> RecordQuerySortAdapter<M> getAdapter(@Nonnull FDBRecordStoreBase<M> recordStore, int maxRecordsToRead) {
        final int memoryLimit = Math.min(maxRecordsToRead, RecordQuerySortAdapter.DEFAULT_MAX_RECORD_COUNT_IN_MEMORY);
        final boolean memoryOnly = memoryLimit == maxRecordsToRead;
        return new RecordQuerySortAdapter<>(memoryLimit, memoryOnly, MemorySortAdapter.OrderComparator::new, this,
                recordStore);
    }

    /**
     * Get an adapter used for a single invocation of a plan using this key.
     * @param <M> type used to represent stored records
     * @param recordStore the record store against which the plan is running
     * @return a new sort adapter specifically made for a damming operation ({@link RecordQueryDamPlan})
     */
    @Nonnull
    public <M extends Message> RecordQuerySortAdapter<M> getAdapterForDam(@Nonnull FDBRecordStoreBase<M> recordStore) {
        return new RecordQuerySortAdapter<>(RecordQuerySortAdapter.DEFAULT_MAX_RECORD_COUNT_IN_MEMORY, true,
                MemorySortAdapter.InsertionOrderComparator::new, this, recordStore);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, key, reverse);
    }

    @Override
    public String toString() {
        return reverse ? key + " DESC" : key.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RecordQuerySortKey that = (RecordQuerySortKey)o;

        if (reverse != that.reverse) {
            return false;
        }
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        return result;
    }

    @Nonnull
    @Override
    public PRecordQuerySortKey toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQuerySortKey.newBuilder().setKey(key.toKeyExpression()).setReverse(reverse).build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static RecordQuerySortKey fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PRecordQuerySortKey recordQuerySortKeyProto) {
        Verify.verify(recordQuerySortKeyProto.hasReverse());
        return new RecordQuerySortKey(KeyExpression.fromProto(Objects.requireNonNull(recordQuerySortKeyProto.getKey())),
                recordQuerySortKeyProto.getReverse());
    }
}
