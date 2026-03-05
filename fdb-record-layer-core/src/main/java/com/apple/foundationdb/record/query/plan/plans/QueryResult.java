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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ProtoSerializable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PQueryResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
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
import java.util.Objects;
import java.util.Optional;

/**
 * QueryResult is the general result that encapsulates the data that is flowed up from plan to plan as well as from
 * plan to consumer. The datum flowed is opaque to some extent but does adhere to a very limited set of common API.
 * For instance, many flowed items intrinsically carry a record, either directly fetched from disk or created as
 * part of a query. Most of the accessors to the wrapped datum cast the datum before returning it. It is the
 * responsibility of the planner to ensure that these casts cannot fail during the execution time of a query.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryResult implements ProtoSerializable {
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
    private PQueryResult cachedProto;

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

    @Nonnull
    public static QueryResult from(@Nullable final Descriptors.Descriptor descriptor, @Nonnull final PQueryResult parsed) {
        try {
            if (parsed.hasPrimitive()) {
                return QueryResult.ofComputed(PlanSerialization.protoToValueObject(parsed.getPrimitive()));
            } else {
                return QueryResult.ofComputed(DynamicMessage.parseFrom(Verify.verifyNotNull(descriptor), parsed.getComplex()));
            }
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid bytes", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(parsed.toByteArray()));
        }
    }


    @Nonnull
    public static QueryResult from(@Nullable final Descriptors.Descriptor descriptor, @Nonnull final ByteString byteString) {
        try {
            return from(descriptor, PQueryResult.parseFrom(byteString));
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid bytes", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(byteString.toByteArray()));
        }
    }

    @Nonnull
    public static QueryResult from(@Nullable final Descriptors.Descriptor descriptor, @Nonnull final byte[] unparsed) {
        return from(descriptor, ZeroCopyByteString.wrap(unparsed));
    }

    /**
     * Create a new result with the given element.
     * @param computed the given computed result
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult ofComputed(@Nullable final Object computed) {
        return new QueryResult(computed, null, null);
    }

    /**
     * Create a new result with the given element while inheriting other parts from a caller-provided entities.
     * @param computed the given computed result
     * @param primaryKey a primary key (if available) or {@code null}
     * @return the newly created query result
     */
    @Nonnull
    public static QueryResult ofComputed(@Nullable final Object computed, @Nullable final Tuple primaryKey) {
        return new QueryResult(computed, null, primaryKey);
    }

    /**
     * Create a new queriedRecord with the given element. This will make a copy in order to extract data from the
     * {@link FDBQueriedRecord} that is not in the underlying protobuf message. More details about that information
     * in the {@link PseudoField} enum.
     *
     * @param resultType type of final value to construct
     * @param evaluationContext context to use (mainly for the type repository)
     * @param queriedRecord the given queriedRecord
     * @return the newly created query queriedRecord
     * @see PseudoField for information on the fields that are copied out of the queried record but are not in the
     *    main definition
     */
    @Nonnull
    public static QueryResult fromQueriedRecord(@Nonnull Type resultType, @Nonnull EvaluationContext evaluationContext, @Nullable final FDBQueriedRecord<?> queriedRecord) {
        if (queriedRecord == null) {
            return new QueryResult(null, null, null);
        }

        final TypeRepository typeRepository = evaluationContext.getTypeRepository();
        final Message datum;
        if (!typeRepository.containsType(resultType) || !(resultType instanceof Type.Record)) {
            // Type not in repository. Just return underlying queriedRecord
            datum = queriedRecord.getRecord();
        } else {
            // Copy over data from the underlying message
            final Message.Builder builder = Objects.requireNonNull(typeRepository.newMessageBuilder(resultType));
            MessageHelpers.deepCopyMessage(builder, queriedRecord.getRecord());

            // Extract any pseudo-fields
            Type.Record recordType = (Type.Record) resultType;
            for (PseudoField pseudoField : PseudoField.values()) {
                pseudoField.fillInIfApplicable(recordType, queriedRecord, builder);
            }

            datum = builder.build();
        }

        return new QueryResult(datum, queriedRecord, queriedRecord.getPrimaryKey());
    }

    @Nonnull
    @Override
    public PQueryResult toProto() {
        if (cachedProto == null) {
            final var builder = PQueryResult.newBuilder();
            if (datum instanceof FDBQueriedRecord) {
                builder.setComplex(((FDBQueriedRecord<?>)datum).getRecord().toByteString());
            } else if (datum instanceof Message) {
                builder.setComplex(((Message)datum).toByteString());
            } else {
                builder.setPrimitive(PlanSerialization.valueObjectToProto(datum));
            }
            cachedProto = builder.buildPartial();
        }
        return cachedProto;
    }
}
