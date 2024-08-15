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

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.TestRecords1EvolvedAgainProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests relating to building/opening record stores, or having multiple
 * copies of the same store on the same context (see: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/489">Issue #489</a>).
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreOpeningTest extends FDBRecordStoreTestBase {

    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreOpeningTest.class);

    @Test
    void open() throws Exception {
        // This tests the functionality of "open", so doesn't use the same method of opening
        // the record store that other methods within this class use.
        KeySpacePath metaDataPath = pathManager.createPath(TestKeySpace.META_DATA_STORE);
        Subspace expectedSubspace;
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            expectedSubspace = path.toSubspace(context);
            metaDataSubspace = metaDataPath.toSubspace(context);
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

            FDBRecordStore recordStore = storeBuilder(context, metaDataBuilder).createOrOpen();
            assertTrue(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertTrue(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This stops the index build.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertTrue(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getDisabledIndexNames());
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

            FDBRecordStore recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertTrue(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
                    .open();
            assertTrue(recordStore.isVersionChanged());
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
            assertTrue(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getDisabledIndexNames());
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

            FDBRecordStore recordStore = storeBuilder(context, metaDataBuilder).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this ran checkVersion.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = storeBuilder(context, metaDataBuilder).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            recordStore = recordStore.asBuilder().setMetaDataProvider(staleMetaData).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
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

            FDBRecordStore recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
                    .uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            int version = origMetaData.getVersion();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this used checkVersion

            final FDBMetaDataStore staleMetaDataStore = metaDataStore;

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path).setMetaDataStore(metaDataStore).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            // The stale meta-data store uses the cached meta-data, hence the old version in the final assert
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(staleMetaDataStore).uncheckedOpen();
            assertFalse(recordStore.isVersionChanged());
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
        }
    }

    @Test
    void testUpdateRecords() {
        KeySpacePath metaDataPath = pathManager.createPath(TestKeySpace.META_DATA_STORE);
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            metaDataSubspace = metaDataPath.toSubspace(context);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            RecordMetaData origMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            final int version = origMetaData.getVersion();

            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            FDBRecordStore recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
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
            final FDBRecordStore recordStoreWithNoLocalFileDescriptor = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
                    .open();
            assertEquals(version + 1, recordStoreWithNoLocalFileDescriptor.getRecordMetaData().getVersion());
            MetaDataException e = assertThrows(MetaDataException.class, () -> recordStoreWithNoLocalFileDescriptor.saveRecord(evolvedRecord));
            assertEquals(e.getMessage(), "descriptor did not match record type");

            // Update the records with a local descriptor. Storing an evolved record must succeed this time.
            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1EvolvedProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
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
            recordStore = storeBuilder(context, origMetaData)
                    .setMetaDataStore(metaDataStore)
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
    void testReadYourWritesWithHeaderUserField() throws Exception {
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
    void testHeaderUserFieldNotUpdatedInRecordStoreOnSameSubspace() throws Exception {
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
    void testGetHeaderFieldOnUninitializedStore() throws Exception {
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
    void storeExistenceChecks() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            FDBRecordStore.Builder storeBuilder = storeBuilder(context, metaData);
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
    void storeExistenceChecksWithNoRecords() throws Exception {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            storeBuilder = storeBuilder(context, metaData);
            FDBRecordStore store = storeBuilder.create();
            // delete the header
            store.ensureContextActive().clear(getStoreInfoKey(store));
            commit(context);
        }

        // Should be able to recover from a completely empty record store
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            FDBRecordStore store = storeBuilder.createOrOpen();

            // put a range subspace in for an index so that a future store opening can see it
            store.ensureContextActive().clear(getStoreInfoKey(store));
            Index foundIndex = metaData.getAllIndexes()
                    .stream()
                    .findAny()
                    .orElseGet(() -> fail("no indexes defined in meta-data"));
            IndexingRangeSet.forIndexBuild(store, foundIndex)
                    .insertRangeAsync(null, null)
                    .get();

            // re-delete the header
            store.ensureContextActive().clear(getStoreInfoKey(store));

            commit(context);
        }
        // Should fail if it finds just the range subspace with default store existence checks, but it
        // should be recoverable using ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES
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

        // Delete everything except a value in the index build space
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore store = storeBuilder.setContext(context).open();
            final Subspace subspace = OnlineIndexer.indexBuildScannedRecordsSubspace(store, metaData.getIndex("MySimpleRecord$str_value_indexed"));
            context.ensureActive().set(subspace.getKey(), FDBRecordStore.encodeRecordCount(1215)); // set a key in the INDEX_BUILD_SPACE
            context.ensureActive().clear(store.getSubspace().getKey(), subspace.getKey());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
        }
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context).createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES);
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
            RecordStoreNoInfoAndNotEmptyException err = assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES));
            assertNotNull(err.getCause());
            assertThat(err.getCause(), instanceOf(RecordCoreException.class));
            assertEquals("Unrecognized keyspace: unknown_keyspace", err.getCause().getMessage());
            commit(context);
        }
    }

    @ParameterizedTest
    @EnumSource(FDBRecordStoreBase.StoreExistenceCheck.class)
    void failIfMissingHeader(FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            storeBuilder = storeBuilder(context, metaData);
            FDBRecordStore store = storeBuilder.create();
            store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("foo")
                    .build());
            // delete the header
            store.ensureContextActive().clear(getStoreInfoKey(store));
            commit(context);
        }

        // Attempt to open a store with no header but with record data. Should fail unless run with the NONE existence check
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            if (existenceCheck == FDBRecordStoreBase.StoreExistenceCheck.NONE) {
                FDBRecordStore store = storeBuilder.createOrOpen(existenceCheck);
                assertNotNull(store.getRecordStoreState().getStoreHeader());
            } else {
                RecordStoreNoInfoAndNotEmptyException noInfoErr = assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> storeBuilder.createOrOpen(existenceCheck));
                assertThat(noInfoErr.getMessage(), containsString("Record store has no info but is not empty"));
                assertThat(noInfoErr.getLogInfo(), hasKey(LogMessageKeys.KEY.toString()));
                Object firstKey = noInfoErr.getLogInfo().get(LogMessageKeys.KEY.toString());
                assertThat(firstKey, instanceOf(Tuple.class));
                long firstKeyBegin = ((Tuple) firstKey).getLong(0);
                assertEquals(FDBRecordStoreKeyspace.RECORD.key(), firstKeyBegin);
            }

            // Clear out record data so that the first element seen is an index entry
            FDBRecordStore store = storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            store.getContext().clear(getStoreInfoKey(store));
            store.getContext().clear(store.recordsSubspace().range(Tuple.from(1066L)));

            commit(context);
        }

        // Attempt to open a store with no header but with index data. Should fail unless run with the NONE existence check
        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            if (existenceCheck == FDBRecordStoreBase.StoreExistenceCheck.NONE) {
                FDBRecordStore store = storeBuilder.createOrOpen(existenceCheck);
                assertNotNull(store.getRecordStoreState().getStoreHeader());
            } else {
                RecordStoreNoInfoAndNotEmptyException noInfoErr = assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> storeBuilder.createOrOpen(existenceCheck));
                assertThat(noInfoErr.getMessage(), containsString("Record store has no info but is not empty"));
                assertThat(noInfoErr.getLogInfo(), hasKey(LogMessageKeys.KEY.toString()));
                Object firstKey = noInfoErr.getLogInfo().get(LogMessageKeys.KEY.toString());
                assertThat(firstKey, instanceOf(Tuple.class));
                long firstKeyBegin = ((Tuple) firstKey).getLong(0);
                assertEquals(FDBRecordStoreKeyspace.INDEX.key(), firstKeyBegin);
            }

            // Now disable all indexes so the first key will not be record or index data
            FDBRecordStore store = storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            for (Index index : store.getRecordMetaData().getAllIndexes()) {
                store.markIndexDisabled(index).join();
            }
            store.ensureContextActive().clear(getStoreInfoKey(store));

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            storeBuilder.setContext(context);
            if (existenceCheck == FDBRecordStoreBase.StoreExistenceCheck.NONE || existenceCheck == FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES) {
                FDBRecordStore store = storeBuilder.createOrOpen(existenceCheck);
                assertNotNull(store.getRecordStoreState().getStoreHeader());
            } else {
                RecordStoreNoInfoAndNotEmptyException noInfoErr = assertThrows(RecordStoreNoInfoAndNotEmptyException.class, () -> storeBuilder.createOrOpen(existenceCheck));
                assertThat(noInfoErr.getMessage(), containsString("Record store has no info or records but is not empty"));
                assertThat(noInfoErr.getLogInfo(), hasKey(LogMessageKeys.KEY.toString()));
                Object firstKey = noInfoErr.getLogInfo().get(LogMessageKeys.KEY.toString());
                assertThat(firstKey, instanceOf(Tuple.class));
                long firstKeyBegin = ((Tuple) firstKey).getLong(0);
                assertEquals(FDBRecordStoreKeyspace.INDEX_STATE_SPACE.key(), firstKeyBegin);
            }
        }
    }

    @Test
    void existenceChecks() throws Exception {
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
    void metaDataVersionZero() {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
        metaData.setVersion(0);

        final FDBRecordStoreBase.UserVersionChecker newStore = (oldUserVersion, oldMetaDataVersion, metaData1) -> {
            assertEquals(-1, oldUserVersion);
            assertEquals(-1, oldMetaDataVersion);
            return CompletableFuture.completedFuture(0);
        };

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder(context, metaData)
                    .setUserVersionChecker(newStore)
                    .create();
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
            recordStore = storeBuilder(context, metaData)
                    .setUserVersionChecker(oldStore)
                    .open();
            commit(context);
        }
    }

    @Test
    void invalidMetaData() {
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
    void conflictWithHeaderChange() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
        RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getVersion(), lessThan(metaData2.getVersion()));

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = storeBuilder(context, metaData1).create();
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }

        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            FDBRecordStore recordStore1 = storeBuilder(context1, metaData1).open();
            assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

            FDBRecordStore recordStore2 = storeBuilder(context2, metaData2).open();
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
            assertThrows(RecordStoreStaleMetaDataVersionException.class,
                    () -> storeBuilder(context, metaData1).open());

            FDBRecordStore recordStore = storeBuilder(context, metaData2).open();
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }
    }

    private static class SizeBasedUserVersionChecker implements FDBRecordStoreBase.UserVersionChecker {
        private final IndexState stateToReturn;
        private final AtomicInteger checkSizeCount;
        private final AtomicLong size;

        public SizeBasedUserVersionChecker(IndexState stateToReturn) {
            this.stateToReturn = stateToReturn;
            this.checkSizeCount = new AtomicInteger();
            this.size = new AtomicLong(-1L);
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
            return CompletableFuture.completedFuture(storeHeader.getUserVersion());
        }

        @Deprecated
        @Override
        public CompletableFuture<Integer> checkUserVersion(final int oldUserVersion, final int oldMetaDataVersion, final RecordMetaDataProvider metaData) {
            throw new RecordCoreException("deprecated checkUserVersion called");
        }

        @Override
        public IndexState needRebuildIndex(final Index index, final long recordCount, final boolean indexOnNewRecordTypes) {
            return fail("should not call count-based needRebuildIndexMethod on size-based user version checker");
        }

        @Nonnull
        @Override
        public CompletableFuture<IndexState> needRebuildIndex(final Index index,
                                                              final Supplier<CompletableFuture<Long>> lazyRecordCount,
                                                              final Supplier<CompletableFuture<Long>> lazyEstimatedSize,
                                                              final boolean indexOnNewRecordTypes) {
            return lazyEstimatedSize.get().thenApply(recordsSize -> {
                checkSizeCount.incrementAndGet();
                size.updateAndGet(currentSize -> {
                    if (currentSize < 0) {
                        return recordsSize;
                    } else {
                        assertEquals(currentSize, recordsSize,
                                "records size should be the same each time for a single store opening");
                        return currentSize;
                    }
                });
                // Because the read records size is non-deterministic, just always return the give state, but
                // only after checking the size
                return stateToReturn;
            });
        }

        long getCheckSizeCount() {
            return checkSizeCount.get();
        }
    }

    @Test
    void sizeBasedUserVersionChecker() {
        final Index universalCountIndex = new Index("countIndex", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(universalCountIndex);
        final RecordMetaData metaData1 = metaDataBuilder.getRecordMetaData();

        // Open the store and make sure estimate size path is used
        try (FDBRecordContext context = openContext()) {
            final SizeBasedUserVersionChecker userVersionChecker = new SizeBasedUserVersionChecker(IndexState.READABLE);
            final FDBRecordStore recordStore = storeBuilder(context, metaData1)
                    .setUserVersionChecker(userVersionChecker)
                    .create();

            assertThat("should have checked the size at least once",
                    userVersionChecker.getCheckSizeCount(), greaterThan(0L));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.ESTIMATE_SIZE));
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable(), "all indexes should be readable on new store opening");

            commit(context);
        }

        // Re-open the store and make sure the estimate is never used
        try (FDBRecordContext context = openContext()) {
            final SizeBasedUserVersionChecker userVersionChecker = new SizeBasedUserVersionChecker(IndexState.DISABLED);
            final FDBRecordStore recordStore = storeBuilder(context, metaData1)
                    .setUserVersionChecker(userVersionChecker)
                    .open();

            assertEquals(0, userVersionChecker.getCheckSizeCount(), "should not have checked the size on already created store");
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.ESTIMATE_SIZE), "should not have estimated the size on already created store");
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable(), "all indexes should be readable on already created store opening");

            commit(context);
        }

        // Add two indexes
        final String indexName1 = "index1";
        final String indexName2 = "index2";
        metaDataBuilder.addIndex("MySimpleRecord", indexName1, "num_value_2");
        metaDataBuilder.addIndex("MySimpleRecord", indexName2, "num_value_2");
        final RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();

        try (FDBRecordContext context = openContext()) {
            final SizeBasedUserVersionChecker userVersionChecker = new SizeBasedUserVersionChecker(IndexState.DISABLED);
            final FDBRecordStore recordStore = storeBuilder(context, metaData2)
                    .setUserVersionChecker(userVersionChecker)
                    .open();

            assertEquals(2, userVersionChecker.getCheckSizeCount(), "should have checked the size once for each index");
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.ESTIMATE_SIZE), "should have only checked the database for the size once");
            assertEquals(ImmutableSet.of(indexName1, indexName2), recordStore.getRecordStoreState().getDisabledIndexNames());

            commit(context);
        }
    }

    @Test
    void testVersionChangedFlagAfterOpening() {
        // Test call getVersionChanged after unchecked opening a record store
        var metadata = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final int firstVersion = metadata.getVersion();
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .uncheckedOpen();
            // No updates to the storeHeader.
            assertFalse(store.isVersionChanged());
            commit(context);
        }

        // Test call getVersionChanged after manual call to checkVersion
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .uncheckedOpen();
            assertFalse(store.isVersionChanged());
            // Explicit call to checkVersion() sets the metadataVersion in the storeHeader.
            store.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).thenApply(changed -> {
                assertTrue(store.isVersionChanged());
                return null;
            }).join();
            commit(context);
        }

        // Test call getVersionChanged after opening a store with metadata update
        metadata.addUniversalIndex(globalCountIndex());
        final int secondVersion = metadata.getVersion();
        assertThat(secondVersion, greaterThan(firstVersion));
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .createOrOpen();
            // createOrOpen() calls the checkVersion() which bumps up the version.
            assertTrue(store.isVersionChanged());
            commit(context);
        }

        // Test call getVersionChanged after opening an already created store
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .createOrOpen();
            // createOrOpen() calls the checkVersion() but the metadata is already up-to-date
            assertFalse(store.isVersionChanged());
            commit(context);
        }

        // Test call getVersionChanged after opening an already created store with new version of metadata
        Index newIndex = new Index("newIndex", concatenateFields("str_value_indexed", "num_value_3_indexed"));
        // metadata is updated
        metadata.addIndex("MySimpleRecord", newIndex);
        final int thirdVersion = metadata.getVersion();
        assertThat(thirdVersion, greaterThan(secondVersion));
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .uncheckedOpen();
            assertFalse(store.isVersionChanged());
            // checkVersion() will bump up the version
            store.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).thenApply(changed -> {
                assertTrue(store.isVersionChanged());
                return null;
            }).join();
            commit(context);
        }

        // Test call getVersionChanged to check info version changed info is preserved across calls to checkVersion
        Index newIndex2 = new Index("newIndex2", concatenateFields("str_value_indexed", "rec_no"));
        metadata.addIndex("MySimpleRecord", newIndex2);
        final int fourthVersion = metadata.getVersion();
        assertThat(fourthVersion, greaterThan(thirdVersion));
        try (FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(metadata)
                    .setKeySpacePath(path)
                    .uncheckedOpen();
            assertFalse(store.isVersionChanged());
            // The checkVersion() will bump up the metadata version in the first iteration (i=0), while it won't have
            // any effect on the other following calls. However, getVersionChanged() should return true even if there
            // is change in one of the calls.
            for (var i = 0; i < 5; i++) {
                store.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).thenApply(changed -> {
                    assertTrue(store.isVersionChanged());
                    return null;
                }).join();
            }
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

    private FDBRecordStore.Builder storeBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaDataProvider metaDataProvider) {
        return FDBRecordStore.newBuilder()
                .setContext(context)
                .setMetaDataProvider(metaDataProvider)
                .setKeySpacePath(path);
    }

    private static byte[] getStoreInfoKey(@Nonnull FDBRecordStore store) {
        return store.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key());
    }

}
