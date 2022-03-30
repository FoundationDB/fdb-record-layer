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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * QueryResult is the general result that encapsulates the data that is flowed up from plan to consumer. The datum
 * flowed is opaque to some extent but does adhere to a very limited set of common API. For instance,
 * many flowed items intrinsically carry a record, either directly fetched from disk or created as part of a query.
 * Most of the accessors to the wrapped datum cast the datum before returning it. It is the responsibility of the planner
 * to ensure that these casts cannot fail during the execution time of a query.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryResult {
    @Nonnull
    private final Object datum;

    private QueryResult(@Nonnull Object datum) {
        this.datum = datum;
    }

    /**
     * Create a new result with the given element.
     * @param result the given result
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult of(@Nonnull Object result) {
        return new QueryResult(result);
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return the object narrowed to the requested class
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <M extends Message> FDBQueriedRecord<M> getQueriedRecord() {
        return (FDBQueriedRecord<M>)get(FDBQueriedRecord.class);
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @return an optional that potentially contains the object narrowed to the requested class
     */
    @Nonnull
    public <M extends Message> Optional<FDBQueriedRecord<M>> getQueriedRecordMaybe() {
        if (datum instanceof FDBQueriedRecord) {
            return Optional.of(getQueriedRecord());
        }
        return Optional.empty();
    }

    /**
     * Retrieve the wrapped result.
     * @return the wrapped result as an object
     */
    @Nonnull
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
        throw new RecordCoreException("cannot be retrieve message from flowed object");
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> Object getObject() {
        if (datum instanceof FDBQueriedRecord) {
            return ((FDBQueriedRecord<M>)datum).getRecord();
        }
        return datum;
    }
}
