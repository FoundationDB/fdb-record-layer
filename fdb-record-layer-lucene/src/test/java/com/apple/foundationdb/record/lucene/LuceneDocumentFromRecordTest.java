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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.SimpleDocument.Builder builder = TestRecordsTextProto.SimpleDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion");
        TestRecordsTextProto.SimpleDocument partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        assertEquals("suggestion", partialMsg.getText());
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.SimpleDocument.Builder builder = TestRecordsTextProto.SimpleDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(2));
        TestRecordsTextProto.SimpleDocument partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        assertEquals("suggestion", partialMsg.getText());
        // The group field is supposed to be populated because it is part of grouping key
        assertEquals(2L, partialMsg.getGroup());
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MultiDocument.Builder builder = TestRecordsTextProto.MultiDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(2));
        TestRecordsTextProto.MultiDocument partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        assertEquals(1, partialMsg.getTextCount());
        assertEquals("suggestion", partialMsg.getText(0));
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.ComplexDocument.Builder builder = TestRecordsTextProto.ComplexDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(10, "tag1"));
        TestRecordsTextProto.ComplexDocument partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        assertEquals("suggestion", partialMsg.getText());
        // The group field is supposed to be populated because it is part of grouping key
        assertEquals(10L, partialMsg.getGroup());
        // The tag field is supposed to be populated because it is part of grouping key
        assertEquals(1, partialMsg.getTagCount());
        assertEquals("tag1", partialMsg.getTag(0));
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MapDocument.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_value", "suggestion");
        TestRecordsTextProto.MapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.MapDocument.Entry entry = partialMsg.getEntry(0);
        // The suggestion is supposed to show up in value field within repeated entry sub-message
        assertEquals("suggestion", entry.getValue());
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MapDocument.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "k1", "suggestion");
        TestRecordsTextProto.MapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.MapDocument.Entry entry = partialMsg.getEntry(0);
        // The k1 is supposed to show up in the key field within repeated entry sub-message
        assertEquals("k1", entry.getKey());
        // The suggestion is supposed to show up in value field within repeated entry sub-message
        assertEquals("suggestion", entry.getValue());
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

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MapDocument.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "k1", "suggestion", Tuple.from(20));
        TestRecordsTextProto.MapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.MapDocument.Entry entry = partialMsg.getEntry(0);

        // The k1 is supposed to show up in the key field within repeated entry sub-message
        assertEquals("k1", entry.getKey());
        // The suggestion is supposed to show up in value field within repeated entry sub-message
        assertEquals("suggestion", entry.getValue());

        // The group field is supposed to be populated because it is part of grouping key
        assertEquals(20L, partialMsg.getGroup());
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
                Tuple.from(30, "r1"), ImmutableList.of(textField("entry_value", "val"), textField("entry_second_value", "2val"), textField("entry_third_value", "3val")),
                Tuple.from(30, "r2"), ImmutableList.of(textField("entry_value", "nval"), textField("entry_second_value", "2nval"), textField("entry_third_value", "3nval"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MapDocument.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_value", "suggestion", Tuple.from(30, "r1"));
        TestRecordsTextProto.MapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.MapDocument.Entry entry = partialMsg.getEntry(0);

        // The suggestion is supposed to show up in value field within repeated entry sub-message
        assertEquals("suggestion", entry.getValue());
        // The second_value field is not supposed to be populated
        assertFalse(entry.hasSecondValue());
        // The key field within repeated entry sub-message is supposed to be populated because it is part of grouping key
        assertEquals("r1", entry.getKey());

        // The group field is supposed to be populated because it is part of grouping key
        assertEquals(30L, partialMsg.getGroup());
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
                        Tuple.from(40, "en"), ImmutableList.of(textField("entry_value", "first"), textField("entry_second_value", "second"), textField("text2", "extra")),
                        Tuple.from(40, "de"), ImmutableList.of(textField("entry_value", "erste"), textField("entry_second_value", "zweite"), textField("text2", "extra"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.MapDocument.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_second_value", "suggestion", Tuple.from(40, "en"));
        TestRecordsTextProto.MapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.MapDocument.Entry entry = partialMsg.getEntry(0);

        // The suggestion is supposed to show up in second_value field within repeated entry sub-message
        assertEquals("suggestion", entry.getSecondValue());
        // The value field is not supposed to be populated
        assertFalse(entry.hasValue());
        // The key field within repeated entry sub-message is supposed to be populated because it is part of grouping key
        assertEquals("en", entry.getKey());

        // The group field is supposed to be populated because it is part of grouping key
        assertEquals(40L, partialMsg.getGroup());
    }

    @Test
    void mapWithSubMessage() {
        TestRecordsTextProto.NestedMapDocument message = TestRecordsTextProto.NestedMapDocument.newBuilder()
                .setDocId(5)
                .addEntry(TestRecordsTextProto.NestedMapDocument.Entry.newBuilder().setKey("k1").setSubEntry(TestRecordsTextProto.NestedMapDocument.SubEntry.newBuilder().setValue("testValue").build()).build())
                .build();
        KeyExpression index = field("entry", KeyExpression.FanType.FanOut)
                .nest(function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                        concat(field("sub_entry").nest(concat(function(LuceneFunctionNames.LUCENE_TEXT, field("value")), function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")))),
                                field("key"))));
        FDBRecord<Message> record = unstoredRecord(message);

        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        textField("entry_k1_value", "testValue"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.NestedMapDocument.Builder builder = TestRecordsTextProto.NestedMapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_k1_value", "suggestion");
        TestRecordsTextProto.NestedMapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.NestedMapDocument.Entry entry = partialMsg.getEntry(0);

        // The k1 is supposed to be populated for the key field within entry
        assertEquals("k1", entry.getKey());

        // The suggestion is supposed to show up in value field within sub-entry, and the second_value is not populated
        TestRecordsTextProto.NestedMapDocument.SubEntry subEntry = entry.getSubEntry();
        assertEquals("suggestion", subEntry.getValue());
        assertFalse(subEntry.hasSecondValue());
    }

    /**
     * When a schema leads an ambiguity of the parsed path given a concatenated Lucene field, the first path that satisfies the given field will be selected and the correct one could be ignored.
     * In this test, because both the path {entry -> sub_entry -> value} and {entry -> sub_entry -> second_value} could match with the given Lucene field "entry_k1_second_value",
     * and in the first path the key is "k1_second" and in the second one the key is "k1".
     * There is no more information to tell which is the expected one so we just pick up the first one to build the partial record.
     * TODO: Predicate the potential ambiguity when loading a schema and reject it
     */
    @Test
    void mapWithSubMessageWithAmbiguity() {
        TestRecordsTextProto.NestedMapDocument message = TestRecordsTextProto.NestedMapDocument.newBuilder()
                .setDocId(5)
                .addEntry(TestRecordsTextProto.NestedMapDocument.Entry.newBuilder().setKey("k1").setSubEntry(TestRecordsTextProto.NestedMapDocument.SubEntry.newBuilder().setSecondValue("testValue").build()).build())
                .build();
        KeyExpression index = field("entry", KeyExpression.FanType.FanOut)
                .nest(function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                        concat(field("sub_entry").nest(concat(function(LuceneFunctionNames.LUCENE_TEXT, field("value")), function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")))),
                                field("key"))));
        FDBRecord<Message> record = unstoredRecord(message);

        assertEquals(ImmutableMap.of(Tuple.from(), ImmutableList.of(
                        textField("entry_k1_second_value", "testValue"))),
                LuceneDocumentFromRecord.getRecordFields(index, record));

        // Build the partial record message for suggestion
        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        TestRecordsTextProto.NestedMapDocument.Builder builder = TestRecordsTextProto.NestedMapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_k1_second_value", "suggestion");
        TestRecordsTextProto.NestedMapDocument partialMsg = builder.build();

        assertEquals(1, partialMsg.getEntryCount());
        TestRecordsTextProto.NestedMapDocument.Entry entry = partialMsg.getEntry(0);

        // The k1_second is supposed to be populated for the key field under entry
        assertEquals("k1_second", entry.getKey());

        // The suggestion is supposed to show up in value field within sub-entry, instead of second_value
        TestRecordsTextProto.NestedMapDocument.SubEntry subEntry = entry.getSubEntry();
        assertEquals("suggestion", subEntry.getValue());
        assertFalse(subEntry.hasSecondValue());
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
