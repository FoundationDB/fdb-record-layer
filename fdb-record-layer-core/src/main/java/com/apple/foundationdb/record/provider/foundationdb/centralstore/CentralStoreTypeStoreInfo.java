/*
 * CentralStoreTypeStoreInfo.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.centralstore;

import com.apple.foundationdb.record.CentralStoreProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.subspace.Subspace;
import com.google.auto.service.AutoService;
import com.google.protobuf.DescriptorProtos;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Singleton
 */
@AutoService(CentralStoreType.class)
public class CentralStoreTypeStoreInfo implements CentralStoreType {
    @Nonnull
    private static final CentralStoreTypeStoreInfo INSTANCE = new CentralStoreTypeStoreInfo();

    private static final String STORE_SUBSPACE_FIELD_NAME = CentralStoreProto.StoreInfo.getDescriptor()
            .findFieldByNumber(CentralStoreProto.StoreInfo.STORE_SUBSPACE_FIELD_NUMBER)
            .getFullName();

    private static final int STORE_INFO_TYPE_KEY = 0;

    // TODO: Will it be helpful if the user can extend this type to more fields/features? If that's the case, the enum
    // here should probably be an interface.
    enum InfoType {
        INDEX_STATES,
        METADATA_VERSION
    }

    @Nullable
    private BiFunction<Subspace, InfoType, Boolean> enable;

    @Nullable
    private List<Index> indexes;

    @Nonnull
    public static CentralStoreTypeStoreInfo instance() {
        if (!isConfigured()) {
            throw new CentralStoreNotSetUpException("CentralStoreTypeStoreInfo is not configured");
        }
        return INSTANCE;
    }

    private CentralStoreTypeStoreInfo() {}

    // Given the subspace of the store, and the information type, return whether or not it should be tracked in
    // CentralStoreTypeStoreInfo.
    public static CentralStoreTypeStoreInfo config(@Nullable BiFunction<Subspace, InfoType, Boolean> enable,
                                                   @Nullable List<Index> indexes) {
        INSTANCE.enable = enable == null ? (v1, v2) -> true : enable;
        INSTANCE.indexes = indexes == null ? Collections.emptyList() : indexes;
        return instance();
    }

    private static boolean isConfigured() {
        return INSTANCE.enable != null && INSTANCE.indexes != null;
    }

    public static <T> T runIfEnabled(@Nonnull FDBRecordContext context,
                                     @Nonnull Subspace storeSubspace,
                                     @Nonnull InfoType infoType,
                                     @Nonnull Function<? super FDBRecordStore, ? extends T> runnable) {
        if (!isConfigured()) {
            return null;
        }
        final CentralStoreTypeStoreInfo instance = instance();
        if (!instance.enable.apply(storeSubspace, infoType)) {
            return null;
        }
        return CentralStore.runIfConfigured(context, runnable);
    }

    @Nonnull
    @Override
    public DescriptorProtos.DescriptorProto getTypeDescriptor() {
        return CentralStoreProto.StoreInfo.getDescriptor().toProto();
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryFields() {
        return Key.Expressions.field(STORE_SUBSPACE_FIELD_NAME);
    }

    @Override
    public int getRecordTypeKey() {
        return STORE_INFO_TYPE_KEY;
    }

    @Nonnull
    @Override
    public List<Index> getIndexes() {
        return indexes;
    }
}
