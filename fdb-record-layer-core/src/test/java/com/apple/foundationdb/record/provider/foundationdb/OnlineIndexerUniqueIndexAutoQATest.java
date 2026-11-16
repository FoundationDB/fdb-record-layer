/*
 * OnlineIndexerUniqueIndexAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.expressions.Query;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for unique indexes built with {@link OnlineIndexer} into
 * {@link IndexState#READABLE_UNIQUE_PENDING}, and then resolved.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerUniqueIndexAutoQATest extends OnlineIndexerTest {

    @Test
    void deleteRecordsWhereRemovesViolationsOfDeletedRecords() {
        // BUG AQA-3: StandardIndexMaintainer.deleteWhere clears only the index range, not the uniqueness violations of the
        // deleted records, so a READABLE_UNIQUE_PENDING index can never become READABLE. Spec: FDBRecordStoreBase
        // deleteRecordsWhere javadoc, "Delete records and associated index entries"; IndexStateCommitCheck javadoc.
        // deleteRecordsWhere deletes "records and associated index entries". The uniqueness violations of the deleted
        // records must go too, otherwise the READABLE_UNIQUE_PENDING index keeps violations no record write can resolve.
        final Index index = new Index("path_str_unique", concat(field("header").nest("path"), field("str_value")),
                EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.getRecordType("MyRecord")
                .setPrimaryKey(concat(field("header").nest("path"), field("header").nest("rec_no")));
        openMetaData(TestRecordsWithHeaderProto.getDescriptor(), hook);
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 0; recNo < 2; recNo++) {
                for (String path : List.of("a", "b")) {
                    recordStore.saveRecord(TestRecordsWithHeaderProto.MyRecord.newBuilder()
                            .setHeader(TestRecordsWithHeaderProto.HeaderRecord.newBuilder().setPath(path).setRecNo(recNo))
                            .setStrValue(path.equals("a") ? "dup" : "x" + recNo)
                            .build());
                }
            }
            context.commit();
        }
        openMetaData(TestRecordsWithHeaderProto.getDescriptor(), hook.andThen(metaDataBuilder ->
                metaDataBuilder.addIndex("MyRecord", index)));
        disableAll(List.of(index));
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isEqualTo(2);
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("a")));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().join()).isEqualTo(2);
            assertThat(recordStore.scanUniquenessViolations(index).map(RecordIndexUniquenessViolation::getPrimaryKey)
                    .asList().join()).isEmpty();
            context.commit();
        }
    }
}
