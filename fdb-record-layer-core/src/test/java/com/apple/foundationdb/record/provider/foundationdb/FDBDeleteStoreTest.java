/*
 * FDBDeleteStoreTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link FDBRecordStore#deleteStore(FDBRecordContext, KeySpacePath)} and its interactions with other state.
 */
public class FDBDeleteStoreTest extends FDBRecordStoreTestBase {

    @Test
    void testDeleteStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, simpleMetaData(NO_HOOK));
            putARecord(recordStore);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, path);
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            // the store should think it doesn't exist
            assertThrows(RecordStoreDoesNotExistException.class,
                    () -> getStoreBuilder(context, simpleMetaData(NO_HOOK)).open());
        }

        try (FDBRecordContext context = openContext()) {
            assertFalse(path.hasData(context));
            getStoreBuilder(context, simpleMetaData(NO_HOOK)).create(); // should succeed, since there's no longer data
            commit(context);
        }
    }

    @Test
    void testDeleteThenRecreateInSameTransaction() {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, simpleMetaData(NO_HOOK));
            putARecord(recordStore);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, path);
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK)).create();
            assertThat(recordStore.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join(),
                    empty());
            putARecord(recordStore);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK)).open();
            assertThat(recordStore.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().join(),
                    hasSize(1));
        }
    }


    @Test
    void testOpenThenDeleteInSameTransaction() throws Exception {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, simpleMetaData(NO_HOOK));
            putARecord(recordStore);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK)).open();
            // read something out of the store.
            // One example of a reason that you would do this is if the data read implies that the store should be deleted
            // But allowing a store to be deleted when there is an existing reference in memory could cause all sorts
            // of problems. Some issues include:
            // 1. The store may have added pre-commit hooks that assume the store still exists, which could result in
            //    inconsistent data being written in the pre-commit hook.
            // 2. Everything that the store has cached needs to be cleaned up. As a one time change this would be fine
            //    but it puts a maintenance burdon on FDBRecordStore if any caches have to also be managed by deleting.
            // It is possible that this could be cleaned up a bunch by refactoring things such that the garden path is
            // to store all data / operations on the context, and have them scoped by subspace, and then a delete like
            // this, or with any other methods could clear things up.
            final FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(1L));
            assertThrows(FDBRecordStore.RecordStoreOpenedException.class,
                    () -> {
                        FDBRecordStore.deleteStore(context, path);
                        return null;
                    });
        }
    }

    private static void putARecord(FDBRecordStore recordStore) {
        TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L)
                .setStrValueIndexed("abc")
                .setNumValueUnique(123)
                .build();
        recordStore.saveRecord(rec);
    }
}
