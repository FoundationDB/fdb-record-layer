/*
 * QueryPlanUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Utility class for query planning.
 */
public class QueryPlanUtils {
    private QueryPlanUtils() {
    }

    /**
     * The method to get a function from an {@link IndexEntry} to a {@link FDBQueriedRecord} representing a partial record.
     */
    @SuppressWarnings("unchecked")
    public static <M extends Message> Function<IndexEntry, FDBQueriedRecord<M>> getCoveringIndexEntryToPartialRecordFunction(final @Nonnull FDBRecordStoreBase<M> store,
                                                                                                                             final @Nonnull String recordTypeName,
                                                                                                                             final @Nonnull String indexName,
                                                                                                                             final @Nonnull IndexKeyValueToPartialRecord toRecord,
                                                                                                                             final boolean hasPrimaryKey) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordType recordType = metaData.getRecordType(recordTypeName);
        final Index index = metaData.getIndex(indexName);
        final Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();
        return indexEntry -> store.coveredIndexQueriedRecord(index, indexEntry, recordType, (M) toRecord.toRecord(recordDescriptor, indexEntry), hasPrimaryKey);
    }
}
