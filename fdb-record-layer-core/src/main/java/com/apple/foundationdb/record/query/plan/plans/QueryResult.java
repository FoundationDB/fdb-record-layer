/*
 * QueryResult.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * QueryResult is the general result that encapsulates the data that is flowed up from plan to plan as well as from
 * plan to consumer. The datum flowed is opaque to some extent but does adhere to a very limited set of common API.
 * For instance, many flowed items intrinsically carry a record, either directly fetched from disk or created as
 * part of a query. Most of the accessors to the wrapped datum cast the datum before returning it. It is the
 * responsibility of the planner to ensure that these casts cannot fail during the execution time of a query.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryResult {
    @Nullable
    private final Object datum;

    @Nullable
    private final FDBQueriedRecord<?> queriedRecord;

    @Nullable
    private final IndexEntry indexEntry;

    @Nullable
    private final Tuple primaryKey;

    @Nullable
    private final RecordType recordType;

    private QueryResult(@Nullable final Object datum,
                        @Nullable final FDBQueriedRecord<?> queriedRecord,
                        @Nullable final IndexEntry indexEntry,
                        @Nullable final Tuple primaryKey,
                        @Nullable final RecordType recordType) {
        this.datum = datum;
        this.queriedRecord = queriedRecord;
        this.indexEntry = indexEntry;
        this.primaryKey = primaryKey;
        this.recordType = recordType;
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return the object narrowed to the requested class
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> FDBQueriedRecord<M> getQueriedRecord() {
        return (FDBQueriedRecord<M>)queriedRecord;
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return an optional that potentially contains the object narrowed to the requested class
     */
    @Nonnull
    public <M extends Message> Optional<FDBQueriedRecord<M>> getQueriedRecordMaybe() {
        return Optional.ofNullable(getQueriedRecord());
    }

    /**
     * Retrieve the wrapped result.
     * @return the wrapped result as an object
     */
    @Nullable
    public Object getDatum() {
        return datum;
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return the object narrowed to the requested class
     */
    @Nonnull
    public <T> T get(@Nonnull final Class<? extends T> clazz) {
        return clazz.cast(datum);
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return an optional that potentially contains the object narrowed to the requested class
     */
    @Nonnull
    public <T> Optional<T> getMaybe(@Nonnull final Class<? extends T> clazz) {
        if (clazz.isInstance(datum)) {
            return Optional.of(get(clazz));
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    public <M extends Message> M getMessage() {
        if (datum instanceof FDBQueriedRecord) {
            return ((FDBQueriedRecord<M>)datum).getRecord();
        } else if (datum instanceof Message) {
            return (M)datum;
        }
        throw new RecordCoreException("cannot retrieve message from flowed object");
    }

    @SuppressWarnings("unchecked")
    public <M extends Message> Optional<M> getMessageMaybe() {
        if (datum instanceof FDBQueriedRecord) {
            return Optional.of(((FDBQueriedRecord<M>)datum).getRecord());
        } else if (datum instanceof Message) {
            return Optional.of((M)datum);
        }
        return Optional.empty();
    }

    @Nullable
    public IndexEntry getIndexEntry() {
        return indexEntry;
    }

    @Nullable
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    @Nullable
    public RecordType getRecordType() {
        return recordType;
    }

    @Nonnull
    public QueryResult withComputed(@Nullable final Object computed) {
        return new QueryResult(computed, queriedRecord, indexEntry, primaryKey, recordType);
    }

    /**
     * Create a new result with the given element.
     * @param computed the given computed result
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult ofComputed(@Nullable Object computed) {
        return new QueryResult(computed, null, null, null, null);
    }

    /**
     * Create a new queriedRecord with the given element.
     * @param queriedRecord the given queriedRecord
     * @return the newly created query queriedRecord
     */
    @Nonnull
    public static QueryResult fromQueriedRecord(@Nullable FDBQueriedRecord<?> queriedRecord) {
        if (queriedRecord == null) {
            return new QueryResult(null, null, null, null, null);
        }
        return new QueryResult(queriedRecord.getRecord(),
                queriedRecord,
                queriedRecord.getIndexEntry(),
                queriedRecord.getPrimaryKey(),
                queriedRecord.getRecordType());
    }
}
