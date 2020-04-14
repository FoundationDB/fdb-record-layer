/*
 * FDBRecordStoreFormatVersionTest.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.test.Tags;
import com.google.common.base.Charsets;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests related to upgrading/downgrading the format version.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreFormatVersionTest extends FDBRecordStoreTestBase {

    @Test
    public void testFormatVersionUpgrade() throws Exception {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK))
                    .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1)
                    .create();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1, recordStore.getFormatVersion());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK))
                    .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                    .open();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, recordStore.getFormatVersion());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK))
                    .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1)
                    .open();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, recordStore.getFormatVersion());
            commit(context);
        }
    }

    /**
     * Test that accessing the header user fields at earlier format versions is disallowed.
     */
    @Test
    public void testAccessUserFieldAtOldFormatVersion() {
        final String expectedErrMsg = "cannot access header user fields at current format version";
        final RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path).setMetaDataProvider(metaData)
                .setFormatVersion(FDBRecordStore.HEADER_USER_FIELDS_FORMAT_VERSION - 1);
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).create();
            RecordCoreException err = assertThrows(RecordCoreException.class,
                    () -> recordStore.getHeaderUserField("foo"));
            assertEquals(expectedErrMsg, err.getMessage());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();
            RecordCoreException err = assertThrows(RecordCoreException.class,
                    () -> recordStore.setHeaderUserField("foo", "bar".getBytes(Charsets.UTF_8)));
            assertEquals(expectedErrMsg, err.getMessage());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setFormatVersion(FDBRecordStore.INFO_ADDED_FORMAT_VERSION).setContext(context).open();
            assertEquals(FDBRecordStore.HEADER_USER_FIELDS_FORMAT_VERSION - 1, recordStore.getFormatVersion());
            RecordCoreException err = assertThrows(RecordCoreException.class,
                    () -> recordStore.clearHeaderUserField("foo"));
            assertEquals(expectedErrMsg, err.getMessage());
            commit(context);
        }
        // Now try upgrading the format version and validate that the fields can be read
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setFormatVersion(FDBRecordStore.HEADER_USER_FIELDS_FORMAT_VERSION)
                    .setContext(context).open();
            assertEquals(FDBRecordStore.HEADER_USER_FIELDS_FORMAT_VERSION, recordStore.getFormatVersion());
            recordStore.setHeaderUserField("foo", "bar".getBytes(Charsets.UTF_8));
            String val = recordStore.getHeaderUserField("foo").toStringUtf8();
            assertEquals("bar", val);
            recordStore.clearHeaderUserField("foo");
            assertNull(recordStore.getHeaderUserField("foo"));
            commit(context);
        }
    }

}
