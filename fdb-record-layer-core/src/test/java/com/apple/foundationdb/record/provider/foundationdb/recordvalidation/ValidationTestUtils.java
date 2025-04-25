/*
 * ValidationTestUtils.java
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

package com.apple.foundationdb.record.provider.foundationdb.recordvalidation;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ValidationTestUtils {
    @Nonnull
    public static Stream<Integer> formatVersions() {
        return Stream.of(
                FDBRecordStore.RECORD_COUNT_KEY_ADDED_FORMAT_VERSION, // 3
                FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION, // 6
                FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
    }

    public static BitSet toBitSet(final long l) {
        return BitSet.valueOf(new long[] {l});
    }

    @Nonnull
    public static Stream<BitSet> splitsToRemove() {
        return LongStream.range(1, 16).mapToObj(l -> toBitSet(l));
    }

    @Nonnull
    public static FDBRecordStoreTestBase.RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords) {
        return getRecordMetaDataHook(splitLongRecords, true);
    }

    @Nonnull
    public static FDBRecordStoreTestBase.RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords, final boolean storeVersions) {
        return metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            metaData.setStoreRecordVersions(storeVersions);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
    }

    public static byte[] getSplitKey(FDBRecordStore store, Tuple primaryKey, int splitNumber) {
        final Tuple pkAndSplit = primaryKey.add(splitNumber);
        return store.recordsSubspace().pack(pkAndSplit);
    }
}
