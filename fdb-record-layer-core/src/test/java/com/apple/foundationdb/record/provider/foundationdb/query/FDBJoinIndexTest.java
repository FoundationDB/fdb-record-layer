/*
 * FDBJoinIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsGroupedParentChildProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of various join index test behaviors.
 */
@Tag(Tags.RequiresFDB)
public class FDBJoinIndexTest extends FDBRecordStoreTestBase {

    private static final String JOIN_TYPE = "JoinChildren";
    private static final String JOIN_INDEX = "AJoinIndex";

    @Test
    void deleteWhereValue() {
        Data data;
        try (FDBRecordContext context = open(FDBJoinIndexTest::addJoinedValueIndex)) {
            data = createGroupedData();
            context.commit();
        }
        try (FDBRecordContext context = open(FDBJoinIndexTest::addJoinedValueIndex)) {
            recordStore.deleteRecordsWhere(Query.field("group").equalsValue(2));
            context.commit();
        }

        try (FDBRecordContext context = open(FDBJoinIndexTest::addJoinedValueIndex)) {
            final List<Tuple> joinChildren = scanAndSort(JOIN_INDEX);
            assertEquals(joinChildren, data.primaryKeysWithoutGroup(recordStore.getRecordMetaData(), 2));
            context.commit();
        }
    }

    private static Arguments badIndex(String name, JoinBuilder builder) {
        return Arguments.of(name, (RecordMetaDataHook)builder::build);
    }

    public static Stream<Arguments> badIndexes() {
        return Stream.of(
                badIndex("No group join", new JoinBuilder().removeGroupJoin()),
                badIndex("Self group join", new JoinBuilder().selfJoin()),
                badIndex("Left outer join", new JoinBuilder().leftOuterJoin()),
                badIndex("Right outer join", new JoinBuilder().rightOuterJoin()),
                badIndex("Full outer join", new JoinBuilder().leftOuterJoin().rightOuterJoin()),
                badIndex("no group in index", new JoinBuilder().removeGroupFromIndex()),
                badIndex("group in wrong spot", new JoinBuilder().groupInWrongSpot())
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badIndexes")
    @SuppressWarnings("try")
    void cannotDeleteWhereValue(String description, RecordMetaDataHook addJoinIndex) {
        try (FDBRecordContext context = open(addJoinIndex)) {
            assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(Query.field("group").equalsValue(2)));
        }
    }

    @Nonnull
    private List<Tuple> scanAndSort(final String indexName) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(indexName),
                        IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                .map(IndexEntry::getPrimaryKey)
                .asList().join()
                .stream()
                .sorted()
                .collect(Collectors.toList());
    }

    private static void addJoinedValueIndex(RecordMetaDataBuilder builder) {
        new JoinBuilder().build(builder);
    }

    private FDBRecordContext open(RecordMetaDataHook hook) {
        FDBRecordContext context = openContext();
        try {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                    .setRecords(TestRecordsGroupedParentChildProto.getDescriptor());
            metaDataBuilder.getRecordType("MyParentRecord")
                    .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
            metaDataBuilder.getRecordType("MyChildRecord")
                    .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
            hook.apply(metaDataBuilder);
            createOrOpenRecordStore(context, metaDataBuilder.build());
            return context;
        } catch (RuntimeException e) {
            context.close();
            throw e;
        }
    }

    private Data createGroupedData() {
        Data data = new Data();
        for (int group = 0; group < 4; group++) {
            for (int parentDoc = 0; parentDoc < 5; parentDoc++) {
                // Have some parents & children that don't have an associated child/parent respectively
                var parentRecord = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                        .setGroup(group)
                        .setRecNo(parentDoc)
                        .setIntValue(parentDoc * 100 + 3)
                        .setTextValue("Here is my text #" + group + "/" + parentDoc)
                        .setChildRecNo(-1 * parentDoc)
                        .build();
                if (!data.missingParent(parentRecord)) {
                    data.add(parentRecord, recordStore.saveRecord(parentRecord));
                }
                if (!data.missingChild(parentRecord)) {
                    var childRecord = TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                            .setGroup(group)
                            .setRecNo(-1 * parentDoc)
                            .setOtherValue(parentDoc * 1000 + 52)
                            .setStrValue("Children " + group + " grouping " + parentDoc)
                            .build();
                    data.add(childRecord, recordStore.saveRecord(childRecord));
                }
            }
        }
        return data;
    }

    private static class JoinBuilder {

        private boolean removeGroupJoin;
        private boolean selfJoin;
        private boolean leftOuterJoin;
        private boolean rightOuterJoin;
        private boolean removeGroupFromIndex;
        private boolean groupInWrongSpot;

        private void build(RecordMetaDataBuilder builder) {
            final JoinedRecordTypeBuilder joinBuilder = builder.addJoinedRecordType(JOIN_TYPE);
            joinBuilder.addConstituent("parent", builder.getRecordType("MyParentRecord"), leftOuterJoin);
            joinBuilder.addConstituent("child", builder.getRecordType("MyChildRecord"), rightOuterJoin);
            if (!removeGroupJoin) {
                joinBuilder.addJoin("parent", Key.Expressions.field("group"),
                        selfJoin ? "parent" : "child", Key.Expressions.field("group"));
            }
            joinBuilder.addJoin("parent", Key.Expressions.field("child_rec_no"),
                    "child", Key.Expressions.field("rec_no"));
            final ThenKeyExpression concat;
            if (removeGroupFromIndex) {
                concat = Key.Expressions.concat(
                        Key.Expressions.field("parent").nest(Key.Expressions.field("int_value")),
                        Key.Expressions.field("child").nest(Key.Expressions.field("other_value")));
            } else if (groupInWrongSpot) {
                concat = Key.Expressions.concat(
                        Key.Expressions.field("parent").nest(Key.Expressions.field("int_value")),
                        Key.Expressions.field("parent").nest(Key.Expressions.field("group")),
                        Key.Expressions.field("child").nest(Key.Expressions.field("other_value")));
            } else {
                concat = Key.Expressions.concat(
                        Key.Expressions.field("parent").nest(Key.Expressions.field("group")),
                        Key.Expressions.field("parent").nest(Key.Expressions.field("int_value")),
                        Key.Expressions.field("child").nest(Key.Expressions.field("other_value")));
            }

            builder.addIndex(JOIN_TYPE, new Index(JOIN_INDEX, concat));
        }

        public JoinBuilder removeGroupJoin() {
            removeGroupJoin = true;
            return this;
        }

        public JoinBuilder selfJoin() {
            selfJoin = true;
            return this;
        }

        public JoinBuilder leftOuterJoin() {
            leftOuterJoin = true;
            return this;
        }

        public JoinBuilder rightOuterJoin() {
            rightOuterJoin = true;
            return this;
        }

        public JoinBuilder removeGroupFromIndex() {
            removeGroupFromIndex = true;
            return this;
        }

        public JoinBuilder groupInWrongSpot() {
            groupInWrongSpot = true;
            return this;
        }
    }

    private static class Data {
        Map<Tuple, TestRecordsGroupedParentChildProto.MyParentRecord> parents = new HashMap<>();
        Map<Tuple, TestRecordsGroupedParentChildProto.MyChildRecord> children = new HashMap<>();

        public Message add(final TestRecordsGroupedParentChildProto.MyParentRecord parent,
                           final FDBStoredRecord<Message> storedRecord) {
            parents.put(storedRecord.getPrimaryKey(), parent);
            return parent;
        }

        public Message add(final TestRecordsGroupedParentChildProto.MyChildRecord child,
                           final FDBStoredRecord<Message> storedRecord) {
            children.put(storedRecord.getPrimaryKey(), child);
            return child;
        }

        public Collection<Tuple> primaryKeysWithoutGroup(
                final RecordMetaData recordMetaData, final int group) {
            return parents.entrySet().stream()
                    .filter(entry -> !missingChild(entry.getValue()))
                    .filter(entry -> !missingParent(entry.getValue()))
                    .filter(entry -> entry.getValue().getGroup() != group)
                    .map(entry -> {
                        final Tuple syntheticRecordTypeKey = recordMetaData.getSyntheticRecordType(JOIN_TYPE)
                                .getRecordTypeKeyTuple();
                        return Tuple.from(syntheticRecordTypeKey.get(0),
                                entry.getKey().getItems(),
                                Tuple.from(entry.getValue().getGroup(), entry.getValue().getChildRecNo()).getItems());
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }

        public boolean missingParent(final TestRecordsGroupedParentChildProto.MyParentRecord parentRecord) {
            return parentRecord.getRecNo() >= 4 || parentRecord.getGroup() >= 3;
        }

        public boolean missingChild(final TestRecordsGroupedParentChildProto.MyParentRecord parentRecord) {
            return parentRecord.getRecNo() <= 0 || parentRecord.getGroup() <= 0;
        }
    }

}
