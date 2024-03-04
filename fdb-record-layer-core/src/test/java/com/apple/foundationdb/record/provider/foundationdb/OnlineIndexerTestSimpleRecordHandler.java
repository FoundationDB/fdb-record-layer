/*
 * OnlineIndexerTestSimpleRecordHandler.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Record handler to use within online index tests to handle {@link com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord}s.
 */
class OnlineIndexerTestSimpleRecordHandler implements OnlineIndexerTestRecordHandler<TestRecords1Proto.MySimpleRecord> {
    private static final OnlineIndexerTestSimpleRecordHandler INSTANCE = new OnlineIndexerTestSimpleRecordHandler();

    private OnlineIndexerTestSimpleRecordHandler() {
    }

    @Override
    public Descriptors.FileDescriptor getFileDescriptor() {
        return TestRecords1Proto.getDescriptor();
    }

    @Nonnull
    @Override
    public FDBRecordStoreTestBase.RecordMetaDataHook baseHook(final boolean splitLongRecords, @Nullable final Index sourceIndex) {
        return metaDataBuilder -> {
            if (splitLongRecords) {
                metaDataBuilder.setSplitLongRecords(splitLongRecords);
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            }
            if (sourceIndex != null) {
                metaDataBuilder.addIndex("MySimpleRecord", sourceIndex);
            }
        };
    }

    @Nonnull
    @Override
    public FDBRecordStoreTestBase.RecordMetaDataHook addIndexHook(@Nonnull final Index index) {
        return metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
    }

    @Nonnull
    @Override
    public Tuple getPrimaryKey(@Nonnull final TestRecords1Proto.MySimpleRecord message) {
        return Tuple.from(message.getRecNo());
    }

    @Nonnull
    public static OnlineIndexerTestSimpleRecordHandler instance() {
        return INSTANCE;
    }
}
