/*
 * LuceneCustomQueryTest.java
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

package com.apple.foundationdb.record.lucene.query;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneIndexTest;
import com.apple.foundationdb.record.lucene.LuceneRecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import java.util.Set;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LuceneCustomQueryTest extends LuceneIndexTest {


    @Test
    void simpleJoinExample() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1625L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1626L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            Set<Long> joinedEntries = Set.of(1625L, 1626L, 1547L); // 2 Engineer Jokes and a Waylon in Set
            LuceneRecordCursor.fxnLocal.set( (leafReaderContext) -> {
                try {
                    final SortedDocValues sortedDocValues = leafReaderContext.reader().getSortedDocValues("s");
                    return new FilteredDocIdSetIterator(sortedDocValues) {
                        @Override
                        protected boolean match(final int doc) {
                            try {
                                BytesRef ref = sortedDocValues.binaryValue();
                                Tuple tuple = Tuple.fromBytes(ref.bytes, ref.offset, ref.length);
                                return joinedEntries.contains(tuple.getLong(0));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException("runtime");
                }
            });
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN);
            assertEquals(2, indexEntries.getCount().join());
            assertEquals(2, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
        }
    }

}
