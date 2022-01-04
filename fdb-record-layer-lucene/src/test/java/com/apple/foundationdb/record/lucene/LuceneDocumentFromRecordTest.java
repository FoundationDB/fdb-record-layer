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

import javax.annotation.Nullable;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests of conversion of records to grouped documents.
 */
class LuceneDocumentFromRecordTest {

    @Test
    void simple() {
        TestRecordsTextProto.SimpleDocument message = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1)
                .setText("some text")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = function(LuceneFunctionNames.LUCENE_TEXT, field("text"));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(textField("text", "some text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void group() {
        TestRecordsTextProto.SimpleDocument message = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(2)
                .setText("more text")
                .setGroup(2)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = function(LuceneFunctionNames.LUCENE_TEXT, field("text")).groupBy(field("group"));
        assertEquals(ImmutableMap.of(Tuple.from(2), ImmutableList.of(textField("text", "more text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void multi() {
        TestRecordsTextProto.MultiDocument message = TestRecordsTextProto.MultiDocument.newBuilder()
                .setDocId(3)
                .addText("some text")
                .addText("other text")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = function(LuceneFunctionNames.LUCENE_TEXT, field("text", KeyExpression.FanType.FanOut));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        textField("text", "some text"),
                        textField("text", "other text"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void biGroup() {
        TestRecordsTextProto.ComplexDocument message = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setHeader(TestRecordsTextProto.ComplexDocument.Header.newBuilder().setHeaderId(4))
                .setGroup(10)
                .setText("first text")
                .addTag("tag1")
                .addTag("tag2")
                .setText2("second text")
                .setScore(100)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = concat(
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                function(LuceneFunctionNames.LUCENE_TEXT, field("text2")),
                field("score"))
                .groupBy(concat(field("group"), field("tag", KeyExpression.FanType.FanOut)));
        assertEquals(ImmutableMap.of(
                        Tuple.from(10, "tag1"), ImmutableList.of(textField("text", "first text"), textField("text2", "second text"), intField("score", 100)),
                        Tuple.from(10, "tag2"), ImmutableList.of(textField("text", "first text"), textField("text2", "second text"), intField("score", 100))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void uncorrelatedMap() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(5)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k1").setValue("v1"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k2").setValue("v2"))
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = field("entry", KeyExpression.FanType.FanOut).nest(concat(field("key"), function(LuceneFunctionNames.LUCENE_TEXT, field("value"))));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        stringField("entry_key", "k1"),
                        textField("entry_value", "v1"),
                        stringField("entry_key", "k2"),
                        textField("entry_value", "v2"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void map() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(5)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k1").setValue("v1"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k2").setValue("v2"))
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = function(LuceneFunctionNames.LUCENE_FIELD_NAME, concat(
                field("entry", KeyExpression.FanType.FanOut).nest(function(LuceneFunctionNames.LUCENE_FIELD_NAME, concat(function(LuceneFunctionNames.LUCENE_TEXT, field("value")), field("key")))),
                value(null)));
        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        textField("k1", "v1"),
                        textField("k2", "v2"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void groupedMap() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(6)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k1").setValue("v10"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("k2").setValue("v20"))
                .setGroup(20)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = function(LuceneFunctionNames.LUCENE_FIELD_NAME, concat(
                field("entry", KeyExpression.FanType.FanOut).nest(function(LuceneFunctionNames.LUCENE_FIELD_NAME, concat(function(LuceneFunctionNames.LUCENE_TEXT, field("value")), field("key")))),
                value(null)))
                .groupBy(field("group"));
        assertEquals(ImmutableMap.of(Tuple.from(20), ImmutableList.of(
                        textField("k1", "v10"),
                        textField("k2", "v20"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void groupingMap() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(7)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("r1").setValue("val").setSecondValue("2val").setThirdValue("3val"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("r2").setValue("nval").setSecondValue("2nval").setThirdValue("3nval"))
                .setGroup(30)
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = new GroupingKeyExpression(concat(field("group"),
                field("entry", KeyExpression.FanType.FanOut)
                        .nest(concat(field("key"),
                                function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
                                function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")),
                                function(LuceneFunctionNames.LUCENE_TEXT, field("third_value"))))), 3);
        assertEquals(ImmutableMap.of(
                Tuple.from(30, "r1"), ImmutableList.of(textField("value", "val"), textField("second_value", "2val"), textField("third_value", "3val")),
                Tuple.from(30, "r2"), ImmutableList.of(textField("value", "nval"), textField("second_value", "2nval"), textField("third_value", "3nval"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    @Test
    void groupingMapWithExtra() {
        TestRecordsTextProto.MapDocument message = TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(8)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("en").setValue("first").setSecondValue("second"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("de").setValue("erste").setSecondValue("zweite"))
                .setGroup(40)
                .setText2("extra")
                .build();
        FDBRecord<Message> record = unstoredRecord(message);
        KeyExpression index = new GroupingKeyExpression(concat(
                field("group"),
                field("entry", KeyExpression.FanType.FanOut).nest(concat(field("key"), function(LuceneFunctionNames.LUCENE_TEXT, field("value")), function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")))),
                function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))), 3);
        assertEquals(ImmutableMap.of(
                        Tuple.from(40, "en"), ImmutableList.of(textField("value", "first"), textField("second_value", "second"), textField("text2", "extra")),
                        Tuple.from(40, "de"), ImmutableList.of(textField("value", "erste"), textField("second_value", "zweite"), textField("text2", "extra"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));
    }

    private static FDBRecord<Message> unstoredRecord(Message message) {
        return new UnstoredRecord<>(message);
    }

    private static LuceneDocumentFromRecord.DocumentField documentField(String name, @Nullable Object value, LuceneIndexExpressions.DocumentFieldType type, boolean stored) {
        return new LuceneDocumentFromRecord.DocumentField(name, value, type, stored);
    }

    private static LuceneDocumentFromRecord.DocumentField stringField(String name, String value) {
        return documentField(name, value, LuceneIndexExpressions.DocumentFieldType.STRING, false);
    }

    private static LuceneDocumentFromRecord.DocumentField textField(String name, String value) {
        return documentField(name, value, LuceneIndexExpressions.DocumentFieldType.TEXT, false);
    }


    private static LuceneDocumentFromRecord.DocumentField intField(String name, int value) {
        return documentField(name, value, LuceneIndexExpressions.DocumentFieldType.INT, false);
    }

}
