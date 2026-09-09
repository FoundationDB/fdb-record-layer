/*
 * LuceneIndexSpellCheckQueryPlanTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Tests for the way {@link LuceneIndexSpellCheckQueryPlan} turns an index entry into a record. A spell-check suggestion
 * is not a stored record, so the plan resolves its record type from the index rather than from a name it holds, and
 * reports no primary key. The store is a mock, since the conversion itself only reads meta-data.
 */
class LuceneIndexSpellCheckQueryPlanTest {

    private static final String SIMPLE_DOC = "SimpleDocument";

    @Nonnull
    private static RecordMetaData metaData() {
        final var metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        // the proto leaves this one to the caller, as the other Lucene tests over these records also do
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC)
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "doc_id"));
        metaDataBuilder.addIndex(SIMPLE_DOC, LuceneIndexTestUtils.SPELLCHECK_INDEX);
        return metaDataBuilder.getRecordMetaData();
    }

    @Nonnull
    private static LuceneIndexSpellCheckQueryPlan plan() {
        final var scanParameters = new LuceneScanSpellCheckParameters(ScanComparisons.EMPTY, "visin", false);
        final var plan = LuceneIndexQueryPlan.of(LuceneIndexTestUtils.SPELLCHECK_INDEX.getName(), scanParameters,
                FetchIndexRecords.PRIMARY_KEY, false, null, null);
        Assertions.assertInstanceOf(LuceneIndexSpellCheckQueryPlan.class, plan,
                "a BY_LUCENE_SPELL_CHECK scan should produce the spell-check plan");
        return (LuceneIndexSpellCheckQueryPlan)plan;
    }

    /**
     * A suggestion entry of a non-grouped spell-check index: the key holds the field the suggestion is for followed by
     * the suggested term, and the value holds the score.
     */
    @Nonnull
    private static IndexEntry suggestion(@Nonnull final Index index, @Nonnull final String suggested) {
        return new IndexEntry(index, Tuple.from("text", suggested), Tuple.from(0.8F));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static FDBRecordStoreBase<Message> storeOver(@Nonnull final RecordMetaData metaData) {
        final FDBRecordStoreBase<Message> store = Mockito.mock(FDBRecordStoreBase.class);
        Mockito.when(store.getRecordMetaData()).thenReturn(metaData);
        Mockito.when(store.coveredIndexQueriedRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                        Mockito.anyBoolean()))
                .thenReturn(Mockito.mock(FDBQueriedRecord.class));
        return store;
    }

    @Test
    void anEntryIsDecodedIntoTheIndexedRecordTypeWithNoPrimaryKey() {
        final var metaData = metaData();
        final var index = metaData.getIndex(LuceneIndexTestUtils.SPELLCHECK_INDEX.getName());
        final var store = storeOver(metaData);

        plan().indexEntryToQueriedRecord(store, EvaluationContext.empty(), suggestion(index, "vision"));

        final var recordTypeCaptor = ArgumentCaptor.forClass(RecordType.class);
        final var partialRecordCaptor = ArgumentCaptor.forClass(Message.class);
        final var hasPrimaryKeyCaptor = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(store).coveredIndexQueriedRecord(Mockito.eq(index), Mockito.any(),
                recordTypeCaptor.capture(), partialRecordCaptor.capture(), hasPrimaryKeyCaptor.capture());

        Assertions.assertEquals(SIMPLE_DOC, recordTypeCaptor.getValue().getName(),
                "the record type should come from the index, since the plan holds only an index name");
        Assertions.assertFalse(hasPrimaryKeyCaptor.getValue(),
                "a suggestion does not come from one record, so it carries no primary key");

        final Message partialRecord = partialRecordCaptor.getValue();
        final Descriptors.FieldDescriptor text = partialRecord.getDescriptorForType().findFieldByName("text");
        Assertions.assertEquals("vision", partialRecord.getField(text),
                "the suggested term should be decoded into the field it was suggested for");
    }

    @Test
    void everyEntryOfTheScanIsConverted() {
        final var metaData = metaData();
        final var index = metaData.getIndex(LuceneIndexTestUtils.SPELLCHECK_INDEX.getName());
        final var store = storeOver(metaData);
        final var entries = List.of(suggestion(index, "vision"), suggestion(index, "visit"));

        final RecordCursor<FDBQueriedRecord<Message>> records =
                plan().fetchIndexRecords(store, EvaluationContext.empty(),
                        continuation -> RecordCursor.fromList(entries), null,
                        ExecuteProperties.newBuilder().build());

        Assertions.assertEquals(entries.size(), records.asList().join().size(),
                "every suggestion the scan returns should be converted");
        Mockito.verify(store, Mockito.times(entries.size()))
                .coveredIndexQueriedRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                        Mockito.anyBoolean());
    }
}
