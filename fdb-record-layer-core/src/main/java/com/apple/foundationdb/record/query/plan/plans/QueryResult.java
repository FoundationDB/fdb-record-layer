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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PQueryResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

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

    // TODO: this can be removed because it is (probably) redundant. The `queriedRecord` is mostly kept here to flow
    //       information that already exist in the `datum` such as the primary key constituents. There are however
    //       some extra information that are exclusive to the `queriedRecord` such as the record type and the record
    //       version, and the size, however this information can be carried out through modelling the plan operators
    //       differently such that they include any relevant information (such as record version) in the flown result
    //       (i.e. the `datum`) itself.
    @Nullable
    private final FDBQueriedRecord<?> queriedRecord;

    // TODO: this can be removed because it is redundant. The primary key information is encoded
    //       inside the `datum` object, and can be retrieved by a `Value` that knows how to reconstruct
    //       the primary key from it correctly.
    @Nullable
    private final Tuple primaryKey;

    // transient field that amortizes the calculation of the serialized form of this immutable object.
    @Nullable
    private ByteString cachedByteString;

    // transient field that amortizes the calculation of the serialized form of this immutable object.
    @Nullable
    private byte[] cachedBytes;

    private QueryResult(@Nullable final Object datum,
                        @Nullable final FDBQueriedRecord<?> queriedRecord,
                        @Nullable final Tuple primaryKey) {
        this.datum = datum;
        this.queriedRecord = queriedRecord;
        this.primaryKey = primaryKey;
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @param <M> Protobuf class for the record message type
     * @return the object narrowed to the requested class
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public <M extends Message> FDBQueriedRecord<M> getQueriedRecord() {
        return (FDBQueriedRecord<M>)queriedRecord;
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the giving class.
     * @param <M> Protobuf class for the record message type
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
     * Retrieve the wrapped result by attempting it to cast it to the given class.
     * @param <T> target type
     * @param clazz class object for target type
     * @return the object narrowed to the requested class
     */
    @Nonnull
    public <T> T get(@Nonnull final Class<? extends T> clazz) {
        return clazz.cast(datum);
    }

    /**
     * Retrieve the wrapped result by attempting it to cast it to the given class.
     * @param <T> target type
     * @param clazz class object for target type
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
        if (queriedRecord != null) {
            return queriedRecord.getIndexEntry();
        }
        return null;
    }

    @Nullable
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    @Nullable
    public RecordType getRecordType() {
        if (queriedRecord != null) {
            return queriedRecord.getRecordType();
        }
        return null;
    }

    @Nonnull
    public QueryResult withComputed(@Nullable final Object computed) {
        return new QueryResult(computed, queriedRecord, primaryKey);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <M extends Message> ByteString toByteString() {
        if (cachedByteString == null) {
            final var builder = PQueryResult.newBuilder();
            if (datum instanceof FDBQueriedRecord) {
                builder.setComplex(((FDBQueriedRecord<M>)datum).getRecord().toByteString());
            } else if (datum instanceof Message) {
                builder.setComplex(((Message)datum).toByteString());
            } else {
                builder.setPrimitive(PlanSerialization.valueObjectToProto(datum));
            }
            cachedByteString = builder.build().toByteString();
        }
        return cachedByteString;
    }

    @Nonnull
    public byte[] toBytes() {
        if (cachedBytes == null) {
            cachedBytes = toByteString().toByteArray();
        }
        return cachedBytes;
    }



    @Nonnull
    public static QueryResult deserialize(@Nullable Descriptors.Descriptor descriptor, @Nonnull byte[] bytes) {
        return deserialize(descriptor, ZeroCopyByteString.wrap(bytes));
    }

    @Nonnull
    public static QueryResult deserialize(@Nullable Descriptors.Descriptor descriptor, @Nonnull ByteString byteString) {
        final PQueryResult parsed;
        try {
            parsed = PQueryResult.parseFrom(byteString);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid bytes", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
        } catch (RecordCoreArgumentException ex) {
            throw ex.addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
        }
        if (parsed.hasPrimitive()) {
            return QueryResult.ofComputed(PlanSerialization.protoToValueObject(parsed.getPrimitive()));
        } else {
            try {
                return QueryResult.ofComputed(DynamicMessage.parseFrom(Verify.verifyNotNull(descriptor), parsed.getComplex()));
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid bytes", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
            } catch (RecordCoreArgumentException ex) {
                throw ex.addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
            }
        }
    }

    /**
     * Create a new result with the given element.
     * @param computed the given computed result
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult ofComputed(@Nullable Object computed) {
        return new QueryResult(computed, null, null);
    }

    /**
     * Create a new result with the given element while inheriting other parts from a caller-provided entities.
     * @param computed the given computed result
     * @param primaryKey a primary key (if available) or {@code null}
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult ofComputed(@Nullable Object computed, @Nullable Tuple primaryKey) {
        return new QueryResult(computed, null, primaryKey);
    }

    /**
     * Create a new queriedRecord with the given element.
     * @param queriedRecord the given queriedRecord
     * @return the newly created query queriedRecord
     */
    @Nonnull
    public static QueryResult fromQueriedRecord(@Nullable FDBQueriedRecord<?> queriedRecord) {
        if (queriedRecord == null) {
            return new QueryResult(null, null, null);
        }
        return new QueryResult(queriedRecord.getRecord(),
                queriedRecord,
                queriedRecord.getPrimaryKey());
    }
}
