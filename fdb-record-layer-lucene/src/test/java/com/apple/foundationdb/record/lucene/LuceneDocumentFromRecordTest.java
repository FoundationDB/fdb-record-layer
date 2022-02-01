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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.SimpleDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion");
        Message partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        Descriptors.FieldDescriptor textField = recordDescriptor.findFieldByName("text");
        assertTrue(partialMsg.hasField(textField));
        assertEquals("suggestion", partialMsg.getField(textField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.SimpleDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(2));
        Message partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        Descriptors.FieldDescriptor textField = recordDescriptor.findFieldByName("text");
        assertTrue(partialMsg.hasField(textField));
        assertEquals("suggestion", partialMsg.getField(textField));

        // The group field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor groupField = recordDescriptor.findFieldByName("group");
        assertTrue(partialMsg.hasField(groupField));
        assertEquals(2L, partialMsg.getField(groupField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MultiDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(2));
        Message partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        Descriptors.FieldDescriptor textField = recordDescriptor.findFieldByName("text");
        assertEquals(1, partialMsg.getRepeatedFieldCount(textField));
        assertEquals("suggestion", partialMsg.getRepeatedField(textField, 0));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.ComplexDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "text", "suggestion", Tuple.from(10, "tag1"));
        Message partialMsg = builder.build();

        // The suggestion is supposed to show up in text field
        Descriptors.FieldDescriptor textField = recordDescriptor.findFieldByName("text");
        assertTrue(partialMsg.hasField(textField));
        assertEquals("suggestion", partialMsg.getField(textField));

        // The group field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor groupField = recordDescriptor.findFieldByName("group");
        assertTrue(partialMsg.hasField(groupField));
        assertEquals(10L, partialMsg.getField(groupField));

        // The tag field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor tagField = recordDescriptor.findFieldByName("tag");
        assertEquals(1, partialMsg.getRepeatedFieldCount(tagField));
        assertEquals("tag1", partialMsg.getRepeatedField(tagField, 0));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_value", "suggestion");
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        // The suggestion is supposed to show up in value field within entry sub-message
        TestRecordsTextProto.MapDocument.Entry entry = (TestRecordsTextProto.MapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("value");
        assertTrue(entry.hasField(valueField));
        assertEquals("suggestion", entry.getField(valueField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "k1", "suggestion");
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        TestRecordsTextProto.MapDocument.Entry entry = (TestRecordsTextProto.MapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);

        // The k1 is supposed to show up in the key field within repeated entry sub-message
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("k1", entry.getField(keyField));

        // The suggestion is supposed to show up in value field within repeated entry sub-message
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("value");
        assertTrue(entry.hasField(valueField));
        assertEquals("suggestion", entry.getField(valueField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "k1", "suggestion", Tuple.from(20));
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        TestRecordsTextProto.MapDocument.Entry entry = (TestRecordsTextProto.MapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);

        // The k1 is supposed to show up in the key field within repeated entry sub-message
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("k1", entry.getField(keyField));

        // The suggestion is supposed to show up in value field within repeated entry sub-message
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("value");
        assertTrue(entry.hasField(valueField));
        assertEquals("suggestion", entry.getField(valueField));

        // The group field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor groupField = recordDescriptor.findFieldByName("group");
        assertTrue(partialMsg.hasField(groupField));
        assertEquals(20L, partialMsg.getField(groupField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_value", "suggestion", Tuple.from(30, "r1"));
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        // The suggestion is supposed to show up in value field within repeated entry sub-message
        TestRecordsTextProto.MapDocument.Entry entry = (TestRecordsTextProto.MapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("value");
        assertTrue(entry.hasField(valueField));
        assertEquals("suggestion", entry.getField(valueField));

        // The second_value field is not supposed to be populated
        Descriptors.FieldDescriptor secondValueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("second_value");
        assertFalse(entry.hasField(secondValueField));

        // The key field within repeated entry sub-message is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("r1", entry.getField(keyField));

        // The group field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor groupField = recordDescriptor.findFieldByName("group");
        assertTrue(partialMsg.hasField(groupField));
        assertEquals(30L, partialMsg.getField(groupField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.MapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_second_value", "suggestion", Tuple.from(40, "en"));
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        // The suggestion is supposed to show up in second_value field within repeated entry sub-message
        TestRecordsTextProto.MapDocument.Entry entry = (TestRecordsTextProto.MapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);
        Descriptors.FieldDescriptor secondValueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("second_value");
        assertTrue(entry.hasField(secondValueField));
        assertEquals("suggestion", entry.getField(secondValueField));

        // The value field is not supposed to be populated
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("value");
        assertFalse(entry.hasField(valueField));

        // The key field within repeated entry sub-message is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.MapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("en", entry.getField(keyField));

        // The group field is supposed to be populated because it is part of grouping key
        Descriptors.FieldDescriptor groupField = recordDescriptor.findFieldByName("group");
        assertTrue(partialMsg.hasField(groupField));
        assertEquals(40L, partialMsg.getField(groupField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.NestedMapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_k1_value", "suggestion");
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        // The k1 is supposed to be populated for the key field under entry
        TestRecordsTextProto.NestedMapDocument.Entry entry = (TestRecordsTextProto.NestedMapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.NestedMapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("k1", entry.getField(keyField));

        Descriptors.FieldDescriptor subEntryField = entryField.getMessageType().findFieldByName("sub_entry");
        assertTrue(entry.hasField(subEntryField));

        // The suggestion is supposed to show up in value field within sub-entry
        TestRecordsTextProto.NestedMapDocument.SubEntry subEntry = entry.getSubEntry();
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.NestedMapDocument.SubEntry.getDescriptor().findFieldByName("value");
        assertTrue(subEntry.hasField(valueField));
        assertEquals("suggestion", subEntry.getField(valueField));
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

        Descriptors.Descriptor recordDescriptor = message.getDescriptorForType();
        Message.Builder builder = TestRecordsTextProto.NestedMapDocument.newBuilder();
        LuceneIndexKeyValueToPartialRecordUtils.buildPartialRecord(index, recordDescriptor, builder, "entry_k1_second_value", "suggestion");
        Message partialMsg = builder.build();

        Descriptors.FieldDescriptor entryField = recordDescriptor.findFieldByName("entry");
        assertEquals(1, partialMsg.getRepeatedFieldCount(entryField));

        // The k1_second is supposed to be populated for the key field under entry
        TestRecordsTextProto.NestedMapDocument.Entry entry = (TestRecordsTextProto.NestedMapDocument.Entry) partialMsg.getRepeatedField(entryField, 0);
        Descriptors.FieldDescriptor keyField = TestRecordsTextProto.NestedMapDocument.Entry.getDescriptor().findFieldByName("key");
        assertTrue(entry.hasField(keyField));
        assertEquals("k1_second", entry.getField(keyField));

        Descriptors.FieldDescriptor subEntryField = entryField.getMessageType().findFieldByName("sub_entry");
        assertTrue(entry.hasField(subEntryField));

        // The suggestion is supposed to show up in value field within sub-entry, instead of second_value
        TestRecordsTextProto.NestedMapDocument.SubEntry subEntry = entry.getSubEntry();
        Descriptors.FieldDescriptor valueField = TestRecordsTextProto.NestedMapDocument.SubEntry.getDescriptor().findFieldByName("value");
        assertTrue(subEntry.hasField(valueField));
        assertEquals("suggestion", subEntry.getField(valueField));
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
