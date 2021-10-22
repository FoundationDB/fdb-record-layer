/*
 * TypedRecordSerializer.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Serialize records using default Protobuf serialization using supplied message builder
 * for the union message type and two accessors for the message field corresponding to the
 * known record type.
 * @param <M> generated Protobuf class for the record message type
 * @param <U> generated Protobuf class for the union message
 * @param <B> generated Protobuf class for the union message's builder
 */
@API(API.Status.UNSTABLE)
public class TypedRecordSerializer<M extends Message, U extends Message, B extends Message.Builder>
        extends MessageBuilderRecordSerializerBase<M, U, B> {
    @Nonnull
    private final Descriptors.FieldDescriptor fieldDescriptor;
    @Nonnull
    private final Predicate<U> tester;
    @Nonnull
    private final Function<U, M> getter;
    @Nonnull
    private final BiConsumer<B, M> setter;
    @Nonnull
    private final AtomicReference<String> validRecordType = new AtomicReference<>();

    public TypedRecordSerializer(@Nonnull Descriptors.FieldDescriptor fieldDescriptor,
                                 @Nonnull Supplier<B> builderSupplier,
                                 @Nonnull Predicate<U> tester,
                                 @Nonnull Function<U, M> getter,
                                 @Nonnull BiConsumer<B, M> setter) {
        super(builderSupplier);
        this.fieldDescriptor = fieldDescriptor;
        this.tester = tester;
        this.getter = getter;
        this.setter = setter;

        B builder = builderSupplier.get();
        @SuppressWarnings("unchecked")
        U empty = (U)builder.buildPartial();
        if (tester.test(empty)) {
            throw new RecordCoreArgumentException("tester is satisfied by empty union");
        }
        M rec = getter.apply(empty);
        if (rec != empty.getField(fieldDescriptor)) {
            throw new RecordCoreArgumentException("fieldDescriptor and getter do not match");
        }
        setter.accept(builder, rec);
        @SuppressWarnings("unchecked")
        U full = (U)builder.buildPartial();
        if (!tester.test(full)) {
            throw new RecordCoreArgumentException("tester not satisfied by full union");
        }
    }

    @Override
    protected void setUnionField(@Nonnull RecordMetaData metaData,
                                 @Nonnull RecordType recordType,
                                 @Nonnull B unionBuilder,
                                 @Nonnull M rec) {
        final String typeName = recordType.getName();
        final String valid =  validRecordType.get();
        if (!typeName.equals(valid)) {
            // Because fields are matched by name, valid will be null or this check will fail.
            if (fieldDescriptor != metaData.getUnionFieldForRecordType(recordType)) {
                throw new RecordSerializationException("fieldDescriptor does not match recordType")
                        .addLogInfo("fieldDescriptor", fieldDescriptor.getName())
                        .addLogInfo("recordType", recordType.getName())
                        .addLogInfo("metaDataVersion", metaData.getVersion());
            }
            validRecordType.compareAndSet(valid, typeName);
        }
        setter.accept(unionBuilder, rec);
    }

    @Nonnull
    @Override
    protected M getUnionField(@Nonnull Descriptors.Descriptor unionDescriptor,
                              @Nonnull U storedRecord) {
        if (!tester.test(storedRecord)) {
            throw new RecordSerializationException("Specified union field was not set")
                    .addLogInfo("unionDescriptorFullName", unionDescriptor.getFullName())
                    .addLogInfo("recordType", storedRecord.getDescriptorForType().getName());
        }
        return getter.apply(storedRecord);
    }

}
