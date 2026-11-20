/*
 * RecordMetaDataBuilderAutoQATest.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link RecordMetaDataBuilder} focusing on proto round trips.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class RecordMetaDataBuilderAutoQATest {
    private static final String OUTER = "OuterRecord";
    private static final String MIDDLE = "MiddleRecord";
    private static final String DOUBLE_NESTED = "DoubleNested";
    private static final String MIDDLE_UNNESTED = "MiddleUnnested";
    private static final String PARENT_CONSTITUENT = "parent";

    @Nonnull
    private static RecordMetaData roundTrip(@Nonnull RecordMetaData metaData) {
        final RecordMetaDataProto.MetaData proto = metaData.toProto();
        return RecordMetaData.newBuilder()
                .addDependencies(new Descriptors.FileDescriptor[] {
                        RecordMetaDataProto.getDescriptor(),
                        RecordMetaDataOptionsProto.getDescriptor()
                })
                .setRecords(proto)
                .getRecordMetaData();
    }

    @Nonnull
    private static RecordMetaDataBuilder doubleNestedBuilderWithTwoUnnestedTypes() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsDoubleNestedProto.getDescriptor());
        final UnnestedRecordTypeBuilder doubleNested = builder.addUnnestedRecordType(DOUBLE_NESTED);
        doubleNested.addParentConstituent(PARENT_CONSTITUENT, builder.getRecordType(OUTER));
        doubleNested.addNestedConstituent("middle", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(),
                PARENT_CONSTITUENT, field("many_middle", FanType.FanOut));
        doubleNested.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                "middle", field("inner", FanType.FanOut));

        final UnnestedRecordTypeBuilder middleUnnested = builder.addUnnestedRecordType(MIDDLE_UNNESTED);
        middleUnnested.addParentConstituent(PARENT_CONSTITUENT, builder.getRecordType(MIDDLE));
        middleUnnested.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                PARENT_CONSTITUENT, field("other_middle").nest(field("inner", FanType.FanOut)));
        return builder;
    }

    @Test
    void toProtoRoundTripPreservesMultiTypeIndexOnSyntheticTypes() {
        // BUG AQA-25: RecordMetaData.toProto reads only syntheticRecordType.getIndexes(), so a multi-type index on
        // synthetic types loses its record types and reloads as a universal index. Spec: toProto / setRecords javadoc.
        // A multi-type index over two unnested types must stay on those two types after toProto/setRecords.
        final RecordMetaDataBuilder builder = doubleNestedBuilderWithTwoUnnestedTypes();
        final Index index = new Index("multiSyntheticIndex", field("inner").nest("foo"));
        builder.addMultiTypeIndex(List.of(builder.getIndexableRecordType(DOUBLE_NESTED),
                builder.getIndexableRecordType(MIDDLE_UNNESTED)), index);
        final RecordMetaData metaData = builder.getRecordMetaData();
        assertThat(metaData.hasUniversalIndex("multiSyntheticIndex")).isFalse();

        final RecordMetaDataProto.Index indexProto = metaData.toProto().getIndexesList().stream()
                .filter(proto -> proto.getName().equals("multiSyntheticIndex"))
                .findFirst()
                .orElseThrow();
        assertThat(indexProto.getRecordTypeList()).containsExactlyInAnyOrder(DOUBLE_NESTED, MIDDLE_UNNESTED);

        final RecordMetaData deserialized = roundTrip(metaData);
        final Index deserializedIndex = deserialized.getIndex("multiSyntheticIndex");
        assertThat(deserialized.recordTypesForIndex(deserializedIndex).stream().map(RecordType::getName))
                .containsExactlyInAnyOrder(DOUBLE_NESTED, MIDDLE_UNNESTED);
    }

    @Test
    void removeIndexOnSyntheticTypeRemovesItFromType() {
        // BUG AQA-26: RecordMetaDataBuilder.removeIndex does not remove the index from synthetic record types, which keep
        // it next to a FormerIndex with the same subspace key. Spec: MetaDataValidator.validateCurrentAndFormerIndexes.
        // Removing an index defined on a synthetic type must remove it from the type, so toProto still works.
        final RecordMetaDataBuilder builder = doubleNestedBuilderWithTwoUnnestedTypes();
        builder.addIndex(DOUBLE_NESTED, new Index("syntheticIndex", field("inner").nest("foo")));
        assertThat(builder.getRecordMetaData().getSyntheticRecordType(DOUBLE_NESTED).getIndexes()).hasSize(1);

        builder.removeIndex("syntheticIndex");
        final RecordMetaData metaData = builder.getRecordMetaData();

        assertThat(metaData.hasIndex("syntheticIndex")).isFalse();
        assertThat(metaData.getFormerIndexes()).hasSize(1);
        assertThat(metaData.getSyntheticRecordType(DOUBLE_NESTED).getIndexes()).isEmpty();
        assertThat(roundTrip(metaData).getSyntheticRecordType(DOUBLE_NESTED).getIndexes()).isEmpty();
    }
}
