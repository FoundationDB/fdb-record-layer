/*
 * FDBRecordStoreOpeningTest.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.TestRecords1EvolvedAgainProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests relating to building/opening record stores, or having multiple
 * copies of the same store on the same context (see: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/489">Issue #489</a>).
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreOpeningTest extends FDBRecordStoreTestBase {

    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreOpeningTest.class);

    @Test
    public void open() throws Exception {
        // This tests the functionality of "open", so doesn't use the same method of opening
        // the record store that other methods within this class use.
        Object[] metaDataPathObjects = new Object[]{"record-test", "unit", "metadataStore"};
        KeySpacePath metaDataPath;
        Subspace expectedSubspace;
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            metaDataPath = TestKeySpace.getKeyspacePath(metaDataPathObjects);
            expectedSubspace = path.toSubspace(context);
            metaDataSubspace = metaDataPath.toSubspace(context);
            context.ensureActive().clear(Range.startsWith(metaDataSubspace.pack()));
            context.commit();
        }

        Index newIndex = new Index("newIndex", concatenateFields("str_value_indexed", "num_value_3_indexed"));
        Index newIndex2 = new Index("newIndex2", concatenateFields("str_value_indexed", "rec_no"));
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setNumValue2(42)
                .setStrValueIndexed("value")
                .setNumValue3Indexed(1729)
                .build();

        // Test open without a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).createOrOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This stops the index build.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getWriteOnlyIndexNames());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            final FDBRecordStore.Builder staleBuilder = recordStore.asBuilder().setMetaDataProvider(staleMetaData);
            TestHelpers.assertThrows(RecordStoreStaleMetaDataVersionException.class, staleBuilder::createOrOpen,
                    LogMessageKeys.LOCAL_VERSION.toString(), version + 1,
                    LogMessageKeys.STORED_VERSION.toString(), version + 2);
        }

        // Test open with a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);

            FDBRecordStore.newBuilder().setMetaDataStore(metaDataStore).setContext(context).setKeySpacePath(path)
                    .createOrOpenAsync().handle((store, e) -> {
                        assertNull(store);
                        assertNotNull(e);
                        assertThat(e, instanceOf(CompletionException.class));
                        Throwable cause = e.getCause();
                        assertNotNull(cause);
                        assertThat(cause, instanceOf(FDBMetaDataStore.MissingMetaDataException.class));
                        return null;
                    }).join();

            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            RecordMetaData origMetaData = metaDataBuilder.getRecordMetaData();
            final int version = origMetaData.getVersion();

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .createOrOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(record);

            final FDBMetaDataStore staleMetaDataStore = metaDataStore;

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            metaDataStore.saveRecordMetaData(metaDataBuilder.getRecordMetaData());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace).setMetaDataStore(metaDataStore).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getWriteOnlyIndexNames());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            // The stale meta-data store uses the cached meta-data, hence the stale version exception
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(staleMetaDataStore);
            TestHelpers.assertThrows(RecordStoreStaleMetaDataVersionException.class, storeBuilder::createOrOpen,
                    LogMessageKeys.LOCAL_VERSION.toString(), version + 1,
                    LogMessageKeys.STORED_VERSION.toString(), version + 2);
        }

        // Test uncheckedOpen without a MetaDataStore

        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this ran checkVersion.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            recordStore = recordStore.asBuilder().setMetaDataProvider(staleMetaData).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
        }

        // Test uncheckedOpen with a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);

            FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).uncheckedOpenAsync().handle((store, e) -> {
                        assertNull(store);
                        assertNotNull(e);
                        assertThat(e, instanceOf(CompletionException.class));
                        Throwable cause = e.getCause();
                        assertNotNull(cause);
                        assertThat(cause, instanceOf(FDBMetaDataStore.MissingMetaDataException.class));
                        return null;
                    }).join();

            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            RecordMetaData origMetaData = metaDataBuilder.getRecordMetaData();
            int version = origMetaData.getVersion();

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this used checkVersion

            final FDBMetaDataStore staleMetaDataStore = metaDataStore;

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path).setMetaDataStore(metaDataStore).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            // The stale meta-data store uses the cached meta-data, hence the old version in the final assert
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(staleMetaDataStore).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
        }
    }

    @Test
    public void testUpdateRecords() {
        KeySpacePath metaDataPath;
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            metaDataPath = TestKeySpace.getKeyspacePath("record-test", "unit", "metadataStore");
            metaDataSubspace = metaDataPath.toSubspace(context);
            context.ensureActive().clear(Range.startsWith(metaDataSubspace.pack()));
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            RecordMetaData origMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            final int version = origMetaData.getVersion();

            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .setStrValueIndexed("value")
                    .setNumValue3Indexed(1729)
                    .build();
            recordStore.saveRecord(record);

            // Update the records without a local descriptor. Storing an evolved record must fail.
            final TestRecords1EvolvedProto.MySimpleRecord evolvedRecord = TestRecords1EvolvedProto.MySimpleRecord.newBuilder()
                    .setRecNo(1067L)
                    .setNumValue2(43)
                    .setStrValueIndexed("evolved value")
                    .setNumValue3Indexed(1730)
                    .build();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            final FDBRecordStore recordStoreWithNoLocalFileDescriptor = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 1, recordStoreWithNoLocalFileDescriptor.getRecordMetaData().getVersion());
            MetaDataException e = assertThrows(MetaDataException.class, () -> recordStoreWithNoLocalFileDescriptor.saveRecord(evolvedRecord));
            assertEquals(e.getMessage(), "descriptor did not match record type");

            // Update the records with a local descriptor. Storing an evolved record must succeed this time.
            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1EvolvedProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(evolvedRecord);

            // Evolve the meta-data one more time and use it for local file descriptor. SaveRecord will succeed.
            final TestRecords1EvolvedAgainProto.MySimpleRecord evolvedAgainRecord = TestRecords1EvolvedAgainProto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .setStrValueIndexed("value")
                    .setNumValue3Indexed(1729)
                    .build();
            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1EvolvedAgainProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 3, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(evolvedAgainRecord);
        }
    }

    /**
     * Test that if a header user field is set that it's value can be read in the same transaction (i.e., that it
     * supports read-your-writes).
     */
    @Test
    public void testReadYourWritesWithHeaderUserField() throws Exception {
        final String userField = "my_key";
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.setHeaderUserField(userField, ByteString.copyFromUtf8("my_value"));
            assertEquals("my_value", recordStore.getHeaderUserField(userField).toStringUtf8());
            // do not commit to make sure it is *only* updated at commit time
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertNull(recordStore.getHeaderUserField("my_key"));
            recordStore.setHeaderUserField(userField, ByteString.copyFromUtf8("my_other_value"));
            assertEquals("my_other_value", recordStore.getHeaderUserField(userField).toStringUtf8());

            // Create a new record store to validate that a new record store in the same transaction also sees the value
            // when opened after the value has been changed
            FDBRecordStore secondStore = recordStore.asBuilder().open();
            assertEquals("my_other_value", secondStore.getHeaderUserField(userField).toStringUtf8());

            secondStore.clearHeaderUserField(userField);
            assertNull(secondStore.getHeaderUserField(userField));

            FDBRecordStore thirdStore = recordStore.asBuilder().open();
            assertNull(secondStore.getHeaderUserField(userField));

            commit(context);
        }
    }

    /**
     * This is essentially a bug, but this test exhibits the behavior. Essentially, if you have a two record store
     * objects opened on the same subspace in the same transaction, and then you update a header user field
     * in one, then it isn't updated in the other. There might be a solution that involves all of this "shared state"
     * living in some shared place for all record stores (as the same problem affects, say, index state information),
     * but that is not what the code does right now. See:
     * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/489">Issue #489</a>.
     */
    @Test
    public void testHeaderUserFieldNotUpdatedInRecordStoreOnSameSubspace() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.setHeaderUserField("user_field", new byte[]{0x42});
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertArrayEquals(new byte[]{0x42}, recordStore.getHeaderUserField("user_field").toByteArray());

            FDBRecordStore secondStore = recordStore.asBuilder().open();
            assertArrayEquals(new byte[]{0x42}, secondStore.getHeaderUserField("user_field").toByteArray());

            recordStore.setHeaderUserField("user_field", new byte[]{0x10, 0x66});
            assertArrayEquals(new byte[]{0x10, 0x66}, recordStore.getHeaderUserField("user_field").toByteArray());
            assertArrayEquals(new byte[]{0x42}, secondStore.getHeaderUserField("user_field").toByteArray());

            commit(context);
        }
    }

    @Test
    public void testGetHeaderFieldOnUninitializedStore() throws Exception {
        final String userField = "some_user_field";
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.setHeaderUserField(userField, "my utf-16 string".getBytes(StandardCharsets.UTF_16));
            commit(context);
            storeBuilder = recordStore.asBuilder();
        }
        try (FDBRecordContext context = openContext()) {
            // Do *not* call check version
            FDBRecordStore store = storeBuilder.setContext(context).build();
            UninitializedRecordStoreException err = assertThrows(UninitializedRecordStoreException.class, () -> store.getHeaderUserField(userField));
            assertThat(err.getLogInfo(), hasKey(LogMessageKeys.KEY_SPACE_PATH.toString()));
            logger.info(KeyValueLogMessage.of("uninitialized store exception: " + err.getMessage(), err.exportLogInfo()));
        }
    }

    @Test
    public void storeExistenceChecks() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(metaData);
            assertThrows(RecordStoreDoesNotExistException.class, storeBuilder::open);
            recordStore = storeBuilder.uncheckedOpen();
            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            recordStore.insertRecord(simple.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = recordStore.asBuilder()
                    .setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
            recordStore = storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = recordStore.asBuilder()
                    .setContext(context);
            assertThrows(RecordStoreAlreadyExistsException.class, storeBuilder::create);
            recordStore = storeBuilder.open();
            assertNotNull(recordStore.loadRecord(Tuple.from(1)));
            commit(context);
        }
    }

    @Test
    public void storeExistenceChecksWithNoRecords() throws Exception {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData);
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore store = storeBuilder.setContext(context).create();
            // delete the header
            store.ensureContextActive().clear(getStoreInfoKey(store));
            commit(context);
        }

        // Should be able to recover from an empty record store
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            FDBRecordStore store = storeBuilder.build(); // do not perform checkVersion yet
            assertNull(context.ensureActive().get(getStoreInfoKey(store)).get());
            assertTrue(store.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES).get());
            commit(context);
        }

        // Insert a record, then delete the store header
        try (FDBRecordContext context = openContext()) {
            // open as the previous open with the relaxed existence check should have fixed the store header
            FDBRecordStore store = storeBuilder.setContext(context).open();
            store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build());
            store.ensureContextActive().clear(getStoreInfoKey(store));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES));
            commit(context);
        }

        // Delete the record store, then insert a key at an unknown keyspace
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, path);
            Subspace subspace = path.toSubspace(context);
            context.ensureActive().set(subspace.pack("unknown_keyspace"), Tuple.from("doesn't matter").pack());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
            RecordCoreException err = assertThrows(RecordCoreException.class, () -> storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES));
            assertEquals("Unrecognized keyspace: unknown_keyspace", err.getMessage());
            commit(context);
        }
    }

    @Test
    public void existenceChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            simple.setNumValue2(111);
            recordStore.insertRecord(simple.build());

            TestRecords1Proto.MyOtherRecord.Builder other = TestRecords1Proto.MyOtherRecord.newBuilder();
            other.setRecNo(2);
            other.setNumValue2(222);
            recordStore.insertRecord(other.build());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            simple.setNumValue2(1111);
            assertThrows(RecordAlreadyExistsException.class, () -> recordStore.insertRecord(simple.build()));

            simple.setRecNo(3);
            simple.setNumValue2(3333);
            assertThrows(RecordDoesNotExistException.class, () -> recordStore.updateRecord(simple.build()));

            simple.setRecNo(2);
            simple.setNumValue2(2222);
            assertThrows(RecordTypeChangedException.class, () -> recordStore.updateRecord(simple.build()));

        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.mergeFrom(recordStore.loadRecord(Tuple.from(1L)).getRecord());
            assertEquals(111, simple.getNumValue2());
            simple.setNumValue2(1111);
            recordStore.updateRecord(simple.build());

            simple.clear();
            simple.setRecNo(4);
            simple.setNumValue2(444);
            recordStore.insertRecord(simple.build());

            commit(context);
        }
    }

    @Test
    public void metaDataVersionZero() {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
        metaData.setVersion(0);

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData);

        final FDBRecordStoreBase.UserVersionChecker newStore = (oldUserVersion, oldMetaDataVersion, metaData1) -> {
            assertEquals(-1, oldUserVersion);
            assertEquals(-1, oldMetaDataVersion);
            return CompletableFuture.completedFuture(0);
        };

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).setUserVersionChecker(newStore).create();
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().hasMetaDataversion());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().hasUserVersion());
            commit(context);
        }

        final FDBRecordStoreBase.UserVersionChecker oldStore = (oldUserVersion, oldMetaDataVersion, metaData12) -> {
            assertEquals(0, oldUserVersion);
            assertEquals(0, oldMetaDataVersion);
            return CompletableFuture.completedFuture(0);
        };

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).setUserVersionChecker(oldStore).open();
            commit(context);
        }
    }

    @Test
    public void invalidMetaData() {
        RecordMetaDataHook invalid = metaData -> metaData.addIndex("MySimpleRecord", "no_such_field");
        try (FDBRecordContext context = openContext()) {
            assertThrows(KeyExpression.InvalidExpressionException.class, () -> openSimpleRecordStore(context, invalid));
        }
    }

    /**
     * Validate that if the store header changes then an open record store in another transaction is failed with
     * a conflict.
     */
    @Test
    public void conflictWithHeaderChange() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
        RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getVersion(), lessThan(metaData2.getVersion()));

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .create();
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }

        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            FDBRecordStore recordStore1 = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context1)
                    .setMetaDataProvider(metaData1)
                    .open();
            assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

            FDBRecordStore recordStore2 = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context2)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(metaData2.getVersion(), recordStore2.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context2);

            // Add a write to the first record store to make sure that the conflict are actually checked.
            recordStore1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(1415)
                    .build());
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
        }

        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordStoreStaleMetaDataVersionException.class, () -> FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .open());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }
    }

    @Nonnull
    private FDBMetaDataStore createMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath metaDataPath,
                                                 @Nonnull Subspace metaDataSubspace,
                                                 @Nullable Descriptors.FileDescriptor localFileDescriptor) {
        FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, metaDataPath);
        metaDataStore.setMaintainHistory(false);
        assertEquals(metaDataSubspace, metaDataStore.getSubspace());
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});
        metaDataStore.setLocalFileDescriptor(localFileDescriptor);
        return metaDataStore;
    }

    private static byte[] getStoreInfoKey(@Nonnull FDBRecordStore store) {
        return store.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key());
    }

}
