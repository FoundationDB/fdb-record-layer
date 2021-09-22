/*
 * LuceneGroupingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.UnstoredRecord;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests of conversion of records to grouped documents.
 */
public class LuceneDocumentFromRecordTest {

    @Test
    public void simple() {
        TestRecordsTextProto.SimpleDocument message = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1)
                .setText("some text")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(field("text"));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(documentEntry("text", "some text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void group() {
        TestRecordsTextProto.SimpleDocument message = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(2)
                .setText("more text")
                .setGroup(2)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(field("text").groupBy(field("group")));
        assertEquals(ImmutableMap.of(Tuple.from(2), ImmutableList.of(documentEntry("text", "more text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void multi() {
        TestRecordsTextProto.MultiDocument message = TestRecordsTextProto.MultiDocument.newBuilder()
                .setDocId(3)
                .addText("some text")
                .addText("other text")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(field("text", KeyExpression.FanType.FanOut));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        documentEntry("text", "some text", KeyExpression.FanType.FanOut),
                        documentEntry("text", "other text", KeyExpression.FanType.FanOut))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void biGroup() {
        TestRecordsTextProto.ComplexDocument message = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setHeader(TestRecordsTextProto.ComplexDocument.Header.newBuilder().setHeaderId(4))
                .setGroup(10)
                .setText("first text")
                .addTag("tag1")
                .addTag("tag2")
                .setText2("second text")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(concat(field("text"), field("text2")).groupBy(concat(field("group"), field("tag", KeyExpression.FanType.FanOut))));
        assertEquals(ImmutableMap.of(
                        Tuple.from(10, "tag1"), ImmutableList.of(documentEntry("text", "first text"), documentEntry("text2", "second text")),
                        Tuple.from(10, "tag2"), ImmutableList.of(documentEntry("text", "first text"), documentEntry("text2", "second text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void map() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(5)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k1").setValue("v1"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k2").setValue("v2"))
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(field("entry", KeyExpression.FanType.FanOut).nest(concat(field("key"), field("value"))));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        documentEntry("k1_value", "v1", luceneField("value")),
                        documentEntry("k2_value", "v2", luceneField("value")))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void groupedMap() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(6)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k1").setValue("v10"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k2").setValue("v20"))
                .setGroup(20)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(field("entry", KeyExpression.FanType.FanOut).nest(concat(field("key"), field("value"))).groupBy(field("group")));
        assertEquals(ImmutableMap.of(Tuple.from(20), ImmutableList.of(
                        documentEntry("k1_value", "v10", luceneField("value")),
                        documentEntry("k2_value", "v20", luceneField("value")))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void groupingMap() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(7)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("r1").setValue("val").setSecondValue("2val").setThirdValue("3val"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("r2").setValue("nval").setSecondValue("2nval").setThirdValue("3nval"))
                .setGroup(30)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(new GroupingKeyExpression(concat(field("group"), field("entry", KeyExpression.FanType.FanOut)
                .nest(concat(field("key"), field("value"), field("second_value"), field("third_value")))), 3));
        assertEquals(ImmutableMap.of(
                Tuple.from(30, "r1"), ImmutableList.of(documentEntry("value", "val"), documentEntry("second_value", "2val"), documentEntry("third_value", "3val")),
                Tuple.from(30, "r2"), ImmutableList.of(documentEntry("value", "nval"), documentEntry("second_value", "2nval"), documentEntry("third_value", "3nval"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    public void groupingMapWithExtra() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(8)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("en").setValue("first").setSecondValue("second"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("de").setValue("erste").setSecondValue("zweite"))
                .setGroup(40)
                .setText2("extra")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = luceneKey(new GroupingKeyExpression(concat(
                field("group"),
                field("entry", KeyExpression.FanType.FanOut).nest(concat(field("key"), field("value"), field("second_value"))),
                field("text2")), 3));
        assertEquals(ImmutableMap.of(
                        Tuple.from(40, "en"), ImmutableList.of(documentEntry("value", "first"), documentEntry("second_value", "second"), documentEntry("text2", "extra")),
                        Tuple.from(40, "de"), ImmutableList.of(documentEntry("value", "erste"), documentEntry("second_value", "zweite"), documentEntry("text2", "extra"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    private static FDBRecord<Message> unstoredRecord(Message message) {
        return new UnstoredRecord<>(message);
    }

    private static KeyExpression luceneKey(KeyExpression key) {
        return new LuceneTypeConverter().visit(key);
    }

    private static LuceneFieldKeyExpression luceneField(String name, KeyExpression.FanType fanType) {
        return (LuceneFieldKeyExpression)luceneKey(field(name, fanType));
    }

    private static LuceneFieldKeyExpression luceneField(String name) {
        return luceneField(name, KeyExpression.FanType.None);
    }

    private static LuceneDocumentFromRecord.DocumentEntry documentEntry(String name, Object value, LuceneFieldKeyExpression key) {
        return new LuceneDocumentFromRecord.DocumentEntry(name, value, key);
    }

    private static LuceneDocumentFromRecord.DocumentEntry documentEntry(String name, Object value) {
        return documentEntry(name, value, luceneField(name));
    }

    private static LuceneDocumentFromRecord.DocumentEntry documentEntry(String name, Object value, KeyExpression.FanType fanType) {
        return documentEntry(name, value, luceneField(name, fanType));
    }

}
