/*
 * FDBRecordStoreRepairHeaderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.TestRecords1Proto;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FDBRecordStoreRepairHeaderTest extends FDBRecordStoreTestBase {

    static Stream<Integer> formatVersions() {
        return IntStream.rangeClosed(0, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION).boxed();
    }

    @ParameterizedTest
    @MethodSource("formatVersions")
    void repairMaxVersion(int initialFormatVersion) {
        createInitialStore(initialFormatVersion);
        clearStoreHeader();
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataProvider metaData = simpleMetaData(NO_HOOK);
            final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metaData, path);
            assertThatThrownBy(storeBuilder::createOrOpen)
                    .isInstanceOf(RecordStoreNoInfoAndNotEmptyException.class);
            commit(context);
        }



    }

    private void clearStoreHeader() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataProvider metaData = simpleMetaData(NO_HOOK);
            recordStore = getStoreBuilder(context, metaData, path).createOrOpen();
            context.ensureActive().clear(recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.key()));
            commit(context);
        }
    }

    private void createInitialStore(final int initialFormatVersion) {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataProvider metaData = simpleMetaData(NO_HOOK);
            recordStore = getStoreBuilder(context, metaData, path)
                    .setFormatVersion(initialFormatVersion)
                    .createOrOpen();
            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setStrValueIndexed("a_1")
                    .setNumValueUnique(1)
                    .build();
            recordStore.saveRecord(rec);
            commit(context);
        }
    }
}
