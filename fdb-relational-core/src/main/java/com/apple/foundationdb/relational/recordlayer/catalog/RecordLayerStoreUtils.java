/*
 * RecordLayerStoreUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * Functionality shared by classes in this package accessing RecordLayer.
 */
class RecordLayerStoreUtils {
    /**
     * Open RecordStore over provided context.
     * @param txn Transaction to use opening.
     * @param schemaPath Path to schema to set keyspace context/domain.
     * @param metaDataProvider Metadata provider for this context.
     * @return Open RecordStore over passed context.
     * @throws RelationalException Thrown if problem opening RecordStore.
     */
    static FDBRecordStoreBase<Message> openRecordStore(@Nonnull Transaction txn,
                RelationalKeyspaceProvider.RelationalSchemaPath schemaPath, RecordMetaDataProvider metaDataProvider)
            throws RelationalException {
        try {
            return FDBRecordStore.newBuilder().setKeySpacePath(schemaPath)
                    .setContext(txn.unwrap(FDBRecordContext.class)).setMetaDataProvider(metaDataProvider).open();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }
}
