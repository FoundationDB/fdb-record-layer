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
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    private static final String PARENT_TYPE = "MyParentRecord";
    private static final String CHILD_TYPE = "MyChildRecord";

    @ParameterizedTest
    @BooleanSource
    void deleteWhereValue(boolean reverseJoinOrder) {
        Data data;
        final RecordMetaDataHook metadataHook = builder -> new JoinBuilder().reverseJoinOrder(reverseJoinOrder).build(builder);
        try (FDBRecordContext context = open(metadataHook)) {
            data = createGroupedData();
            context.commit();
        }
        try (FDBRecordContext context = open(metadataHook)) {
            recordStore.deleteRecordsWhere(Query.field("group").equalsValue(2));
            context.commit();
        }

        try (FDBRecordContext context = open(metadataHook)) {
            final List<Tuple> joinChildren = scanAndSort(JOIN_INDEX);
            assertEquals(joinChildren, data.primaryKeysWithoutGroup(recordStore.getRecordMetaData(), 2));
            context.commit();
        }
    }

    private static Arguments badIndex(String name, JoinBuilder builder) {
        return Arguments.of(name, builder);
    }

    public static Stream<Arguments> badIndexes() {
        // Note: Some of these could potentially be supported, but aren't currently because getting the logic correct
        // without accidentally expending it to things that should not be supported is hard
        return Stream.of(
                badIndex("No group join", new JoinBuilder().removeGroupJoin()),
                badIndex("Self group join", new JoinBuilder().selfJoin()),
                badIndex("Left outer join", new JoinBuilder().leftOuterJoin()),
                badIndex("Right outer join", new JoinBuilder().rightOuterJoin()),
                badIndex("Full outer join", new JoinBuilder().leftOuterJoin().rightOuterJoin()),
                badIndex("no group in index", new JoinBuilder().removeGroupFromIndex()),
                badIndex("group in wrong spot", new JoinBuilder().groupInWrongSpot()),
                badIndex("addTypeExpression", new JoinBuilder().addTypeExpression()),
                badIndex("addTypeExpressionAndDeleteByParentType", new JoinBuilder().addTypeExpression().setDeleteByType(PARENT_TYPE)),
                badIndex("addTypeExpressionAndDeleteByChildType", new JoinBuilder().addTypeExpression().setDeleteByType(CHILD_TYPE)),
                badIndex("deleteByParentType", new JoinBuilder().setDeleteByType(PARENT_TYPE)),
                badIndex("deleteByChildType", new JoinBuilder().setDeleteByType(CHILD_TYPE)),
                badIndex("justDeleteByParentType", new JoinBuilder().setDeleteByType(PARENT_TYPE).onlyDeleteByType()),
                badIndex("justDeleteByChildType", new JoinBuilder().setDeleteByType(CHILD_TYPE).onlyDeleteByType())
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badIndexes")
    @SuppressWarnings("try")
    void cannotDeleteWhereValue(String description, JoinBuilder addJoinIndex) {
        try (FDBRecordContext context = open(addJoinIndex::build)) {
            assertThrows(Query.InvalidExpressionException.class,
                    () -> {
                        if (addJoinIndex.deleteByType != null) {
                            if (addJoinIndex.onlyDeleteByType) {
                                recordStore.deleteRecordsWhere(new RecordTypeKeyComparison(addJoinIndex.deleteByType));
                            } else {
                                recordStore.deleteRecordsWhere(addJoinIndex.deleteByType,
                                        Query.field("group").equalsValue(2));
                            }
                        } else {
                            recordStore.deleteRecordsWhere(Query.field("group").equalsValue(2));
                        }
                    });
        }
    }


    public static Stream<Arguments> deleteWhereByJoinType() {
        return Stream.of(
                badIndex("addTypeExpressionAndDeleteByJoinType", new JoinBuilder().addTypeExpression().setDeleteByType(JOIN_TYPE)),
                badIndex("deleteByJoinType", new JoinBuilder().setDeleteByType(JOIN_TYPE)),
                badIndex("justDeleteByJoinType", new JoinBuilder().setDeleteByType(JOIN_TYPE).onlyDeleteByType())
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deleteWhereByJoinType")
    @SuppressWarnings("try")
    void cannotDeleteWhereByJoinType(String description, JoinBuilder addJoinIndex) {
        // it doesn't make sense to delete by a synthetic type
        MatcherAssert.assertThat(addJoinIndex.deleteByType, Matchers.notNullValue());
        try (FDBRecordContext context = open(addJoinIndex::build)) {
            assertThrows(MetaDataException.class,
                    () -> {
                        recordStore.deleteRecordsWhere(addJoinIndex.deleteByType,
                                Query.field("group").equalsValue(2));
                    });
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
        private boolean reverseJoinOrder;
        private boolean addTypeExpression;
        /**
         * An easy means to allow the Arguments to tell the test to delete by the type instead; only currently
         * used by {@link #cannotDeleteWhereValue}.
         */
        private String deleteByType = null;
        private boolean onlyDeleteByType;

        private void build(RecordMetaDataBuilder builder) {
            List<KeyExpression> primaryKeyExpressions = new ArrayList<>();
            if (deleteByType != null) {
                primaryKeyExpressions.add(Key.Expressions.recordType());
            }
            primaryKeyExpressions.add(Key.Expressions.field("group"));
            primaryKeyExpressions.add(Key.Expressions.field("rec_no"));
            final ThenKeyExpression primaryKey = new ThenKeyExpression(primaryKeyExpressions);
            builder.getRecordType(PARENT_TYPE).setPrimaryKey(primaryKey);
            builder.getRecordType(CHILD_TYPE).setPrimaryKey(primaryKey);

            final JoinedRecordTypeBuilder joinBuilder = builder.addJoinedRecordType(JOIN_TYPE);
            joinBuilder.addConstituent("parent", builder.getRecordType(PARENT_TYPE), leftOuterJoin);
            joinBuilder.addConstituent("child", builder.getRecordType(CHILD_TYPE), rightOuterJoin);
            List<Runnable> addJoins = new ArrayList<>();
            if (!removeGroupJoin) {
                addJoins.add(() -> joinBuilder.addJoin("parent", Key.Expressions.field("group"),
                        selfJoin ? "parent" : "child", Key.Expressions.field("group")));
            }
            addJoins.add(() -> joinBuilder.addJoin("parent", Key.Expressions.field("child_rec_no"),
                    "child", Key.Expressions.field("rec_no")));
            if (reverseJoinOrder) {
                Collections.reverse(addJoins);
            }
            addJoins.forEach(Runnable::run);
            List<KeyExpression> thenExpressions = new ArrayList<>();
            if (addTypeExpression) {
                thenExpressions.add(Key.Expressions.recordType());
            }
            final KeyExpression parentIntValue = Key.Expressions.field("parent").nest(Key.Expressions.field("int_value"));
            if (groupInWrongSpot) {
                thenExpressions.add(parentIntValue);
            }
            if (!removeGroupFromIndex) {
                thenExpressions.add(Key.Expressions.field("parent").nest(Key.Expressions.field("group")));
            }
            if (!groupInWrongSpot) {
                thenExpressions.add(parentIntValue);
            }
            thenExpressions.add(Key.Expressions.field("child").nest(Key.Expressions.field("other_value")));
            final ThenKeyExpression concat = new ThenKeyExpression(thenExpressions);

            builder.addIndex(JOIN_TYPE, new Index(JOIN_INDEX, concat));
        }

        public JoinBuilder removeGroupJoin() {
            removeGroupJoin = true;
            return this;
        }

        public JoinBuilder reverseJoinOrder(boolean newValue) {
            reverseJoinOrder = newValue;
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

        public JoinBuilder addTypeExpression() {
            addTypeExpression = true;
            return this;
        }

        public JoinBuilder setDeleteByType(String typeName) {
            deleteByType = typeName;
            return this;
        }

        public JoinBuilder onlyDeleteByType() {
            onlyDeleteByType = true;
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
