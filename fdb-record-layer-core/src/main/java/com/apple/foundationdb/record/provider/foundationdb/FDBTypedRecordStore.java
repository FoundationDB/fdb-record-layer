/*
 * FDBTypedRecordStore.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TypedRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A type-safe record store.
 *
 * Such a store can only contain a single record type, given by means of several method
 * references to its generated protobuf class.
 *
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public class FDBTypedRecordStore<M extends Message> extends FDBRecordStoreBase<M> {

    protected FDBTypedRecordStore(@Nonnull FDBRecordContext context,
                                  @Nonnull SubspaceProvider subspaceProvider,
                                  int defaultOutputFormatVersion,
                                  @Nonnull RecordMetaDataProvider metaDataProvider,
                                  @Nonnull RecordSerializer<M> serializer,
                                  @Nonnull IndexMaintainerRegistry indexMaintainerRegistry,
                                  @Nonnull IndexMaintenanceFilter indexMaintenanceFilter,
                                  @Nonnull FDBRecordStoreBase.PipelineSizer pipelineSizer) {
        super(context, subspaceProvider, defaultOutputFormatVersion, metaDataProvider, serializer,
              indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer);
    }

    /**
     * A builder for {@link FDBTypedRecordStore}.
     * @param <M> generated Protobuf class for the record message type
     * @param <U> generated Protobuf class for the union message
     * @param <B> generated Protobuf class for the union message's builder
     * @see FDBTypedRecordStore#newBuilder
     */
    public static class Builder<M extends Message, U extends Message, B extends Message.Builder> extends FDBRecordStoreBuilder<M, FDBTypedRecordStore<M>> {
        protected Builder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                          @Nonnull Descriptors.FieldDescriptor fieldDescriptor,
                          @Nonnull Supplier<B> builderSupplier,
                          @Nonnull Predicate<U> tester,
                          @Nonnull Function<U, M> getter,
                          @Nonnull BiConsumer<B, M> setter) {
            metaDataProvider = new RecordMetaDataBuilder(fileDescriptor);
            serializer = new TypedRecordSerializer<>(fieldDescriptor, builderSupplier, tester, getter, setter);
        }

        protected Builder(Builder<M, U, B> other) {
            super(other);
        }

        protected Builder(FDBTypedRecordStore<M> store) {
            super(store);
        }

        @Override
        @Nonnull
        public FDBTypedRecordStore<M> build() {
            return new FDBTypedRecordStore<>(context, subspaceProvider, formatVersion, getMetaDataProviderForBuild(),
                    serializer, indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer);
        }

        @Override
        @Nonnull
        public Builder<M, U, B> copyBuilder() {
            return new Builder<>(this);
        }

        // Following Overrides only necessary to narrow the return type.

        @Override
        @Nonnull
        public Builder<M, U, B> setSerializer(@Nonnull RecordSerializer<M> serializer) {
            super.setSerializer(serializer);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setFormatVersion(int formatVersion) {
            super.setFormatVersion(formatVersion);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider) {
            super.setMetaDataProvider(metaDataProvider);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore) {
            super.setMetaDataStore(metaDataStore);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setContext(@Nullable FDBRecordContext context) {
            super.setContext(context);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setSubspace(@Nullable Subspace subspace) {
            super.setSubspace(subspace);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setKeySpacePath(@Nullable KeySpacePath path) {
            super.setKeySpacePath(path);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setUserVersionChecker(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
            super.setUserVersionChecker(userVersionChecker);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry) {
            super.setIndexMaintainerRegistry(indexMaintainerRegistry);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            super.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M, U, B> setPipelineSizer(@Nonnull FDBRecordStoreBase.PipelineSizer pipelineSizer) {
            super.setPipelineSizer(pipelineSizer);
            return this;
        }
    }

    /**
     * Create a new typed record store builder.
     *
     * <pre><code>
     * static final FDBTypedRecordStore.Builder&lt;MyProto.MyRecord, MyProto.RecordTypeUnion, MyProto.RecordTypeUnion.Builder&gt; BUILDER =
     * FDBTypedRecordStore.newBuilder(
     *         MyProto.getDescriptor(),
     *         MyProto.RecordTypeUnion.getDescriptor().findFieldByNumber(MyProto.RecordTypeUnion._MYRECORD_FIELD_NUMBER),
     *         MyProto.RecordTypeUnion::newBuilder,
     *         MyProto.RecordTypeUnion::hasMyRecord,
     *         MyProto.RecordTypeUnion::getMyRecord,
     *         MyProto.RecordTypeUnion.Builder::setMyRecord)
     *
     * final FDBTypedRecordStore&lt;MyProto.MyRecord, MyProto.RecordTypeUnion, MyProto.RecordTypeUnion.Builder&gt; store =
     *     BUILDER.copyBuilder().setContext(ctx).setSubspace(s).createOrOpen();
     * final MyProto.MyRecord myrec1 = store.loadRecord(pkey).getRecord();
     * </code></pre>
     *
     * @param fileDescriptor file descriptor for all record message types
     * @param fieldDescriptor field descriptor for the union field used to hold the target record type
     * @param builderSupplier builder for the union message type
     * @param tester predicate to determine whether an instance of the union message has the target record type
     * @param getter accessor to get record message instance from union message instance
     * @param setter access to store record message instance into union message instance
     * @param <M> generated Protobuf class for the record message type
     * @param <U> generated Protobuf class for the union message
     * @param <B> generated Protobuf class for the union message's builder
     * @return a builder using the given functions
     */
    @Nonnull
    public static <M extends Message, U extends Message, B extends Message.Builder> Builder<M, U, B> newBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                                                                                                @Nonnull Descriptors.FieldDescriptor fieldDescriptor,
                                                                                                                @Nonnull Supplier<B> builderSupplier,
                                                                                                                @Nonnull Predicate<U> tester,
                                                                                                                @Nonnull Function<U, M> getter,
                                                                                                                @Nonnull BiConsumer<B, M> setter) {
        return new Builder<>(fileDescriptor, fieldDescriptor, builderSupplier, tester, getter, setter);
    }

    @Nonnull
    public Builder<M, ?, ?> asBuilder() {
        return new Builder<>(this);
    }

}
