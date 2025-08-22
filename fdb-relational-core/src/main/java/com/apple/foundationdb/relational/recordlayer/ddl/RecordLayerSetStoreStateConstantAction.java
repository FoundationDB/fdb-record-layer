/*
 * RecordLayerSetStoreStateConstantAction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataProvider;
import com.apple.foundationdb.relational.recordlayer.storage.StoreConfig;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import java.net.URI;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerSetStoreStateConstantAction implements ConstantAction {
    private final StoreCatalog catalog;
    private final RecordLayerConfig rlConfig;
    private final URI dbUri;
    private final String schemaName;
    private final KeySpace keySpace;

    public RecordLayerSetStoreStateConstantAction(URI dbUri,
                                                  String schemaName,
                                                  RecordLayerConfig rlConfig,
                                                  KeySpace keySpace,
                                                  StoreCatalog catalog) {
        this.schemaName = schemaName;
        this.catalog = catalog;
        this.dbUri = dbUri;
        this.rlConfig = rlConfig;
        this.keySpace = keySpace;
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //can't without violating Record layer isolation law
    public void execute(Transaction txn) throws RelationalException {
        final var databasePath = RelationalKeyspaceProvider.toDatabasePath(dbUri, keySpace).schemaPath(schemaName);
        try {
            FDBRecordStore recordStore =
                    FDBRecordStore.newBuilder()
                            .setKeySpacePath(databasePath)
                            .setSerializer(StoreConfig.DEFAULT_RELATIONAL_SERIALIZER)
                            .setMetaDataProvider(new CatalogMetaDataProvider(catalog, dbUri, schemaName, txn))
                            .setContext(txn.unwrap(FDBRecordContext.class))
                            .open();
            // set index states
            // Note that markIndexReadable and markIndexReadableOrUniquePending should only be allowed if the index is built, although it might be allowed if the store is empty.
            // It is fine for tests that have less than 200 records, so all the indexes will be automatically built during the call to open above, and thus we're only downgrading the index.
            for (Map.Entry<String, IndexState> indexStateEntry : rlConfig.getIndexStateMap().entrySet()) {
                Boolean markIndex = false;
                switch (indexStateEntry.getValue()) {
                    case READABLE:
                        markIndex = recordStore.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, recordStore.markIndexReadable(indexStateEntry.getKey()));
                        break;
                    case WRITE_ONLY:
                        markIndex = recordStore.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, recordStore.markIndexWriteOnly(indexStateEntry.getKey()));
                        break;
                    case DISABLED:
                        markIndex = recordStore.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, recordStore.markIndexDisabled(indexStateEntry.getKey()));
                        break;
                    case READABLE_UNIQUE_PENDING:
                        markIndex = recordStore.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, recordStore.markIndexReadableOrUniquePending(recordStore.getRecordMetaData().getIndex(indexStateEntry.getKey())));
                        break;
                    default:
                        break;
                }
                assert Boolean.TRUE.equals(markIndex);
            }
        } catch (RecordStoreAlreadyExistsException rsaee) {
            // The schema already exists!
            throw new RelationalException("Schema <" + schemaName + "> already exists", ErrorCode.SCHEMA_ALREADY_EXISTS);
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }
}
