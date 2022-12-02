/*
 * CachedMetaDataStore.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A local cache of RecordMetaData objects, intended to be held by a single Database at a time (and thus holding
 * only a small number of MetaData fields).
 *
 * This is intended to be scoped to a single connection space, and so it is _not_ thread safe.
 *
 * TODO(bfines) this will _mostly_ work, but if the underlying MetaData changes it will cause the upper-level
 * getDataBuilder() objects to break because the schema and the format no longer makes sense, even within a specific
 * transaction. So at some point we'll need to propagate that error to something more user-friendly.
 */
@NotThreadSafe
public class CachedMetaDataStore implements RecordMetaDataStore {
    private final Map<String, CacheCell> cachedProviders = new HashMap<>();
    private final RecordMetaDataStore backingStore;

    public CachedMetaDataStore(RecordMetaDataStore backingStore) {
        this.backingStore = backingStore;
    }

    @Override
    public RecordMetaDataProvider loadMetaData(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) throws RelationalException {
        String key = (dbUri.getPath() + "/" + schemaName).toUpperCase(Locale.ROOT);
        CacheCell cacheCell = cachedProviders.get(key);
        if (cacheCell == null) {
            RecordMetaDataProvider rmdp = backingStore.loadMetaData(txn, dbUri, schemaName);
            cacheCell = new CacheCell(rmdp);
            cachedProviders.put(key, cacheCell);
        }

        if (cacheCell.checkVersion) {
            /*
             * We aren't sure if the MetaData version has changed or not, so we have to check
             */
            final RecordMetaData recordMetaData = backingStore.loadMetaData(txn, dbUri, schemaName).getRecordMetaData();
            int version = recordMetaData.getVersion();
            if (cacheCell.value.getRecordMetaData().getVersion() != version) {
                cacheCell = new CacheCell(recordMetaData);
                cachedProviders.put(key, cacheCell);
            } else {
                cacheCell.checkVersion = false;
            }
        }
        final CacheCell theCell = cacheCell;
        txn.unwrap(RecordContextTransaction.class).addTerminationListener(() -> theCell.checkVersion = true);
        return theCell.value;
    }

    private static class CacheCell {
        private final RecordMetaDataProvider value;
        private boolean checkVersion;

        public CacheCell(RecordMetaDataProvider value) {
            this.value = value;
        }
    }
}
