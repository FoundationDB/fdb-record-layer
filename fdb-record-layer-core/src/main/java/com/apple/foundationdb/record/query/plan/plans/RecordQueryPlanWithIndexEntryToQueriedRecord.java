/*
 * RecordQueryPlanWithIndexEntryToQueriedRecord.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * Trait for a {@link RecordQueryPlan} that reads an index directly and so has to hand back a record rather than the raw
 * {@link IndexEntry}.
 * <p>
 * Not to be confused with {@link IndexKeyValueToPartialRecord}, which this uses: that places an entry's columns into a
 * partial <em>message</em>, whereas the method here produces the {@link FDBQueriedRecord} a plan flows, pairing that
 * message with the index, the record type it is reported as, and whether a primary key survives.
 * </p>
 * <p>
 * Implementors differ in the <em>shape</em> an entry is decoded into, and the two static helpers name that choice:
 * {@link #intoStoredRecordShape} decodes into a partial copy of the record type, {@link #intoShape} into a shape no
 * stored record has -- an aggregate index plan decodes into the result of its select-having, whose aggregate column
 * exists in no record. Left implicit, that choice is how a plan decodes entries into the wrong shape while every
 * individual step looks right.
 * </p>
 * <p>
 * The pieces are passed as arguments rather than bundled, because this runs once per index entry read and should avoid
 * allocation when feasible.
 * </p>
 */
@API(API.Status.INTERNAL)
public interface RecordQueryPlanWithIndexEntryToQueriedRecord extends RecordQueryPlan {

    /**
     * Converts one entry of this plan's index into a queried record, via {@link #intoStoredRecordShape} or
     * {@link #intoShape}.
     * @param store the store the query runs against
     * @param context the evaluation context
     * @param indexEntry the entry to convert
     * @param <M> the Protobuf message type of a record
     * @return the queried record the entry converts to
     */
    @Nonnull
    <M extends Message> FDBQueriedRecord<M> indexEntryToQueriedRecord(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nonnull IndexEntry indexEntry);

    /**
     * Converts an entry into a partial copy of {@code recordType}.
     * @param store the store the query runs against
     * @param index the index the entry came from
     * @param recordType the record type to decode into a partial copy of
     * @param converter the converter placing entry columns into that record
     * @param hasPrimaryKey whether the converted entry carries the primary key of a record
     * @param indexEntry the entry to convert
     * @param <M> the Protobuf message type of a record
     * @return the queried record the entry converts to
     */
    @Nonnull
    static <M extends Message> FDBQueriedRecord<M> intoStoredRecordShape(@Nonnull final FDBRecordStoreBase<M> store,
                                                                        @Nonnull final Index index,
                                                                        @Nonnull final RecordType recordType,
                                                                        @Nonnull final IndexKeyValueToPartialRecord converter,
                                                                        final boolean hasPrimaryKey,
                                                                        @Nonnull final IndexEntry indexEntry) {
        return intoShape(store, index, recordType, recordType.getDescriptor(), converter, hasPrimaryKey, indexEntry);
    }

    /**
     * Converts an entry into {@code shape}, which need not be the shape of any stored record. The result is still
     * reported as {@code recordType}.
     * @param store the store the query runs against
     * @param index the index the entry came from
     * @param recordType the record type the converted record is reported as
     * @param shape the descriptor to decode the entry into
     * @param converter the converter placing entry columns into {@code shape}
     * @param hasPrimaryKey whether the converted entry carries the primary key of a record
     * @param indexEntry the entry to convert
     * @param <M> the Protobuf message type of a record
     * @return the queried record the entry converts to
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    static <M extends Message> FDBQueriedRecord<M> intoShape(@Nonnull final FDBRecordStoreBase<M> store,
                                                            @Nonnull final Index index,
                                                            @Nonnull final RecordType recordType,
                                                            @Nonnull final Descriptors.Descriptor shape,
                                                            @Nonnull final IndexKeyValueToPartialRecord converter,
                                                            final boolean hasPrimaryKey,
                                                            @Nonnull final IndexEntry indexEntry) {
        return store.coveredIndexQueriedRecord(index, indexEntry, recordType,
                (M)converter.toRecord(shape, indexEntry), hasPrimaryKey);
    }
}
