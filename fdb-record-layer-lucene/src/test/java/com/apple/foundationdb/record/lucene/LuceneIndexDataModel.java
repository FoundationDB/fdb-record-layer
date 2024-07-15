/*
 * LuceneIndexModel.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsGroupedParentChildProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

/**
 * Model for creating a lucene appropriate dataset with various configurations.
 */
public class LuceneIndexDataModel {

    @Nonnull
    static Tuple saveRecords(final FDBRecordStore recordStore,
                             final boolean isSynthetic,
                             final int group,
                             final int countInGroup,
                             final long timestamp,
                             final RandomTextGenerator textGenerator,
                             final Random random) {
        var parent = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                .setGroup(group)
                .setRecNo(1001L + countInGroup)
                .setTimestamp(timestamp)
                .setTextValue(isSynthetic ? "This is not the text that goes in lucene"
                              : textGenerator.generateRandomText("about"))
                .setIntValue(random.nextInt())
                .setChildRecNo(1000L - countInGroup)
                .build();
        Tuple primaryKey;
        if (isSynthetic) {
            var child = TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                    .setGroup(group)
                    .setRecNo(1000L - countInGroup)
                    .setStrValue(textGenerator.generateRandomText("forth"))
                    .setOtherValue(random.nextInt())
                    .build();
            final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                    .getSyntheticRecordType("JoinChildren")
                    .getRecordTypeKeyTuple();
            primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                    recordStore.saveRecord(parent).getPrimaryKey().getItems(),
                    recordStore.saveRecord(child).getPrimaryKey().getItems());
        } else {
            primaryKey = recordStore.saveRecord(parent).getPrimaryKey();
        }
        return primaryKey;
    }

    @Nonnull
    static Index addIndex(final boolean isSynthetic, final KeyExpression rootExpression, final Map<String, String> options, final RecordMetaDataBuilder metaDataBuilder) {
        Index index;
        index = new Index("joinNestedConcat", rootExpression, LuceneIndexTypes.LUCENE, options);

        if (isSynthetic) {
            final JoinedRecordTypeBuilder joinBuilder = metaDataBuilder.addJoinedRecordType("JoinChildren");
            joinBuilder.addConstituent("parent", "MyParentRecord");
            joinBuilder.addConstituent("child", "MyChildRecord");
            joinBuilder.addJoin("parent", Key.Expressions.field("group"),
                    "child", Key.Expressions.field("group"));
            joinBuilder.addJoin("parent", Key.Expressions.field("child_rec_no"),
                    "child", Key.Expressions.field("rec_no"));
            metaDataBuilder.addIndex("JoinChildren", index);
        } else {
            metaDataBuilder.addIndex("MyParentRecord", index);
        }
        return index;
    }

    @Nonnull
    static KeyExpression createRootExpression(final boolean isGrouped, final boolean isSynthetic) {
        ThenKeyExpression baseExpression;
        KeyExpression groupingExpression;
        if (isSynthetic) {
            baseExpression = Key.Expressions.concat(
                    Key.Expressions.field("parent")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_STORED,
                                    Key.Expressions.field("int_value"))),
                    Key.Expressions.field("child")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_TEXT,
                                    Key.Expressions.field("str_value"))),
                    Key.Expressions.field("parent")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_SORTED,
                                    Key.Expressions.field("timestamp")))
            );
            groupingExpression = Key.Expressions.field("parent").nest("group");
        } else {
            baseExpression = Key.Expressions.concat(
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_STORED,
                            Key.Expressions.field("int_value")),
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_TEXT,
                            Key.Expressions.field("text_value")),
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_SORTED,
                            Key.Expressions.field("timestamp"))
            );
            groupingExpression = Key.Expressions.field("group");
        }
        KeyExpression rootExpression;
        if (isGrouped) {
            rootExpression = baseExpression.groupBy(groupingExpression);
        } else {
            rootExpression = baseExpression;
        }
        return rootExpression;
    }

    @Nonnull
    static RecordMetaDataBuilder createBaseMetaDataBuilder() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsGroupedParentChildProto.getDescriptor());
        metaDataBuilder.getRecordType("MyParentRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        metaDataBuilder.getRecordType("MyChildRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        return metaDataBuilder;
    }
}
