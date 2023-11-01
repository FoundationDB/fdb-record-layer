/*
 * UnnestedRecordTypeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;


import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsImportedMapProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests of the {@link UnnestedRecordType} class. Some of these tests may require access to an underlying FDB record store.
 */
@Tag(Tags.RequiresFDB)
class UnnestedRecordTypeTest extends FDBRecordStoreTestBase {
    @Nonnull
    private static final String OUTER = "OuterRecord";
    @Nonnull
    private static final String UNNESTED_MAP = "UnnestedMap";
    @Nonnull
    private static final String TWO_UNNESTED_MAPS = "TwoUnnestedMaps";
    @Nonnull
    private static final String DOUBLE_NESTED = "DoubleNested";

    @Nonnull
    private static final KeyExpression ENTRIES_FAN_OUT = field("map").nest(field("entry", FanType.FanOut));
    @Nonnull
    private static final String INNER_FOO_OUTER_INNER_BAR_INDEX = "innerFooOuterInnerBar";

    private static RecordMetaData mapMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    private static RecordMetaData doubleNestedMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsDoubleNestedProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    private static RecordMetaData importedMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsImportedMapProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    private static RecordMetaDataHook addMapType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED_MAP, OUTER);
            typeBuilder.addNestedConstituent("map_entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    UnnestedRecordType.PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
        };
    }

    private static RecordMetaDataHook addTwoMapsType() {
        return metaDataBuilder -> {
            UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(TWO_UNNESTED_MAPS, OUTER);
            typeBuilder.addNestedConstituent("entry_one", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    UnnestedRecordType.PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
            typeBuilder.addNestedConstituent("entry_two", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    UnnestedRecordType.PARENT_CONSTITUENT, ENTRIES_FAN_OUT);
        };
    }

    private static RecordMetaDataHook addDoubleNestedType() {
        return metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(DOUBLE_NESTED, OUTER);
            typeBuilder.addNestedConstituent("middle", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.getDescriptor(),
                    UnnestedRecordType.PARENT_CONSTITUENT, field("many_middle", FanType.FanOut));
            typeBuilder.addNestedConstituent("inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    "middle", field("inner", FanType.FanOut));
            typeBuilder.addNestedConstituent("outer_inner", TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    UnnestedRecordType.PARENT_CONSTITUENT, field("inner", FanType.FanOut));
        };
    }

    private static RecordMetaDataHook addInnerFooOuterInnerBarIndex() {
        return metaDataBuilder -> metaDataBuilder.addIndex(DOUBLE_NESTED, new Index(INNER_FOO_OUTER_INNER_BAR_INDEX, concat(field("inner").nest("foo"), field("outer_inner").nest("bar"))));
    }

    private TestRecordsNestedMapProto.OuterRecord sampleMapRecord() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1066)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar"))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("baz").setValue("qux"))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("wid").setValue("get"))
                )
                .build();
    }

    private TestRecordsNestedMapProto.OuterRecord sampleMapRecordWithDuplicateEntries() {
        return TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(1415L)
                .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar"))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("wow").setValue("zaa"))
                        .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder().setKey("foo").setValue("bar"))
                )
                .build();
    }

    private Collection<TestRecordsNestedMapProto.OuterRecord> sampleMapRecords() {
        return List.of(sampleMapRecord(), sampleMapRecordWithDuplicateEntries());
    }

    private TestRecordsDoubleNestedProto.OuterRecord sampleDoubleNestedRecord() {
        return TestRecordsDoubleNestedProto.OuterRecord.newBuilder()
                .setRecNo(1066)
                .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                        .setFoo(1L)
                        .setBar("one")
                )
                .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                        .setFoo(2L)
                        .setBar("two")
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(3L)
                                .setBar("three")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(4L)
                                .setBar("four")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(5L)
                                .setBar("five")
                        )
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(6L)
                                .setBar("six")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(7L)
                                .setBar("seven")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(8L)
                                .setBar("eight")
                        )
                )
                .addManyMiddle(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.newBuilder()
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(9L)
                                .setBar("nine")
                        )
                        .addInner(TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord.newBuilder()
                                .setFoo(10L)
                                .setBar("ten")
                        )
                )
                .build();
    }

    @Test
    void mapTypeToAndFromProto() {
        final RecordMetaData metaData = mapMetaData(addMapType());
        assertProtoSerializationSuccessful(metaData, UNNESTED_MAP);
    }

    @Test
    void twoMapsToAndFromProto() {
        final RecordMetaData metaData = mapMetaData(addTwoMapsType());
        assertProtoSerializationSuccessful(metaData, TWO_UNNESTED_MAPS);
    }

    @Test
    void doubleNestedToAndFromProto() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType());
        assertProtoSerializationSuccessful(metaData, DOUBLE_NESTED);
    }

    @Test
    void importedMapTypeToAndFromProto() {
        final RecordMetaData metaData = importedMetaData(addMapType());
        assertProtoSerializationSuccessful(metaData, UNNESTED_MAP);
    }

    @Test
    void importedTwoMapsToAndFromProto() {
        final RecordMetaData metaData = importedMetaData(addTwoMapsType());
        assertProtoSerializationSuccessful(metaData, TWO_UNNESTED_MAPS);
    }

    private static void assertProtoSerializationSuccessful(RecordMetaData metaData, String unnestedTypeName) {
        final UnnestedRecordType type = (UnnestedRecordType) metaData.getSyntheticRecordType(unnestedTypeName);

        RecordMetaDataProto.MetaData proto = metaData.toProto();
        RecordMetaData fromProto = RecordMetaData.build(proto);
        UnnestedRecordType typeFromProto = (UnnestedRecordType) fromProto.getSyntheticRecordType(unnestedTypeName);
        assertEquals(type.getPrimaryKey(), typeFromProto.getPrimaryKey(), "types should have the same primary key");

        assertEquals(type.getRecordTypeKey(), typeFromProto.getRecordTypeKey(), "record type keys should match");
        SyntheticRecordType.Constituent parentConstituent = typeFromProto.getConstituents().stream()
                .filter(constituent -> constituent.getName().equals(UnnestedRecordType.PARENT_CONSTITUENT))
                .findFirst()
                .orElseGet(() -> fail("did not find parent constituent"));
        RecordType outerTypeFromProto = fromProto.getRecordType(OUTER);
        assertSame(outerTypeFromProto, parentConstituent.getRecordType(), "parent constituent had incorrect object");

        assertEquals(type.getConstituents().size(), typeFromProto.getConstituents().size(), "types should have same number of constituents");
        for (SyntheticRecordType.Constituent constituent : type.getConstituents()) {
            SyntheticRecordType.Constituent constituentFromProto = typeFromProto.getConstituents().stream()
                    .filter(c -> c.getName().equals(constituent.getName()))
                    .findFirst()
                    .orElseGet(() -> fail("missing constituent " + constituent.getName()));
            // Descriptors may not be equal because protobuf descriptors don't implement a simple equals, but they should
            // serialize to the same thing
            assertEquals(constituent.getRecordType().getDescriptor().toProto(), constituentFromProto.getRecordType().getDescriptor().toProto());
        }

        assertEquals(type.getNestings().size(), typeFromProto.getNestings().size());
        for (int i = 0; i < type.getNestings().size(); i++) {
            UnnestedRecordType.Nesting nesting = type.getNestings().get(i);
            UnnestedRecordType.Nesting nestingFromProto = typeFromProto.getNestings().get(i);
            assertEquals(nesting.getParentConstituent().getName(), nestingFromProto.getParentConstituent().getName());
            assertEquals(nesting.getChildConstituent().getName(), nestingFromProto.getChildConstituent().getName());
            assertEquals(nesting.getNestingExpression(), nestingFromProto.getNestingExpression());
        }
    }

    @Test
    void loadMapType() {
        final RecordMetaData metaData = mapMetaData(addMapType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(UNNESTED_MAP);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i));
                    FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                    assertNotNull(synthetic);
                    assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                    assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                    assertEquals(outerRecord, synthetic.getConstituent(UnnestedRecordType.PARENT_CONSTITUENT).getRecord());
                    assertEquals(entry, synthetic.getConstituent("map_entry").getRecord());
                }
            }

            commit(context);
        }
    }

    @Test
    void loadTwoMapsType() {
        final RecordMetaData metaData = mapMetaData(addTwoMapsType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(TWO_UNNESTED_MAPS);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            for (TestRecordsNestedMapProto.OuterRecord outerRecord : sampleMapRecords()) {
                FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    final TestRecordsNestedMapProto.MapRecord.Entry entry1 = outerRecord.getMap().getEntry(i);
                    for (int j = 0; j < outerRecord.getMap().getEntryCount(); j++) {
                        final TestRecordsNestedMapProto.MapRecord.Entry entry2 = outerRecord.getMap().getEntry(j);

                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(i), Tuple.from(j));
                        FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                        assertNotNull(synthetic);
                        assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                        assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                        assertEquals(outerRecord, synthetic.getConstituent(UnnestedRecordType.PARENT_CONSTITUENT).getRecord());
                        assertEquals(entry1, synthetic.getConstituent("entry_one").getRecord());
                        assertEquals(entry2, synthetic.getConstituent("entry_two").getRecord());
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void loadDoubleNestedType() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType());
        final RecordType unnestedType = metaData.getSyntheticRecordType(DOUBLE_NESTED);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            TestRecordsDoubleNestedProto.OuterRecord outerRecord = sampleDoubleNestedRecord();
            FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

            for (int i = 0; i < outerRecord.getInnerCount(); i++) {
                final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord outerInnerRecord = outerRecord.getInner(i);
                for (int j = 0; j < outerRecord.getManyMiddleCount(); j++) {
                    final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middleRecord = outerRecord.getManyMiddle(j);
                    for (int k = 0; k < middleRecord.getInnerCount(); k++) {
                        final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord innerRecord = middleRecord.getInner(k);

                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(j), Tuple.from(k), Tuple.from(i));
                        FDBSyntheticRecord synthetic = recordStore.loadSyntheticRecord(syntheticPrimaryKey).join();
                        assertNotNull(synthetic);
                        assertEquals(syntheticPrimaryKey, synthetic.getPrimaryKey());
                        assertEquals(syntheticPrimaryKey, unnestedType.getPrimaryKey().evaluateMessageSingleton(synthetic, synthetic.getRecord()).toTuple());

                        assertEquals(outerRecord, synthetic.getConstituent(UnnestedRecordType.PARENT_CONSTITUENT).getRecord());
                        assertEquals(middleRecord, synthetic.getConstituent("middle").getRecord());
                        assertEquals(innerRecord, synthetic.getConstituent("inner").getRecord());
                        assertEquals(outerInnerRecord, synthetic.getConstituent("outer_inner").getRecord());
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void indexDoubleNestedType() {
        final RecordMetaData metaData = doubleNestedMetaData(addDoubleNestedType().andThen(addInnerFooOuterInnerBarIndex()));
        final RecordType unnestedType = metaData.getSyntheticRecordType(DOUBLE_NESTED);
        final Index index = metaData.getIndex(INNER_FOO_OUTER_INNER_BAR_INDEX);

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            TestRecordsDoubleNestedProto.OuterRecord outerRecord = sampleDoubleNestedRecord();
            FDBStoredRecord<Message> stored = recordStore.saveRecord(outerRecord);

            final List<IndexEntry> entryList = new ArrayList<>();
            for (int i = 0; i < outerRecord.getInnerCount(); i++) {
                final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord outerInnerRecord = outerRecord.getInner(i);
                for (int j = 0; j < outerRecord.getManyMiddleCount(); j++) {
                    final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord middleRecord = outerRecord.getManyMiddle(j);
                    for (int k = 0; k < middleRecord.getInnerCount(); k++) {
                        final TestRecordsDoubleNestedProto.OuterRecord.MiddleRecord.InnerRecord innerRecord = middleRecord.getInner(k);
                        final Tuple syntheticPrimaryKey = Tuple.from(unnestedType.getRecordTypeKey(), stored.getPrimaryKey(), Tuple.from(j), Tuple.from(k), Tuple.from(i));

                        entryList.add(new IndexEntry(index, Tuple.from(innerRecord.getFoo(), outerInnerRecord.getBar()).addAll(syntheticPrimaryKey), TupleHelpers.EMPTY, syntheticPrimaryKey));
                    }
                }
            }
            entryList.sort(Comparator.comparing(IndexEntry::getKey));

            List<IndexEntry> entries = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            assertEquals(entryList, entries);

            commit(context);
        }
    }
}
