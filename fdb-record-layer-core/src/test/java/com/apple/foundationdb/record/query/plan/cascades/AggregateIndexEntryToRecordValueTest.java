/*
 * AggregateIndexEntryToRecordValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Experiment for <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2907">issue 2907</a>: reading an
 * aggregate index entry into a record by evaluating a single {@link RecordConstructorValue}, rather than by running the
 * copiers of an {@code IndexKeyValueToPartialRecord} over a protobuf builder.
 * <p>
 * Both indexes here are defined over the very same key expression, so the only thing separating them is the index type,
 * and with it where each column physically lands in the entry. The same builder produces a value for either, and both
 * evaluate to the same logical record — which is the property that would let one translator serve both.
 * </p>
 * <pre>
 *   record: recNo = 1, numValue2 = 10, numValue3Indexed = 20, numValueUnique = 100
 *
 *   max_ever_long  entry:  key (10, 20)       value (100)
 *   permuted_max   entry:  key (10, 100, 20)  value ()
 *   logical record both:   (group1 = 10, group2 = 20, aggregate = 100)
 * </pre>
 */
@Tag(Tags.RequiresFDB)
class AggregateIndexEntryToRecordValueTest {

    private static final String MAX_EVER_INDEX = "agg_max_ever";
    private static final String PERMUTED_INDEX = "agg_permuted_max";

    private static final String GROUP_1 = "group1";
    private static final String GROUP_2 = "group2";
    private static final String AGGREGATE = "aggregate";

    /**
     * The shape an aggregate index entry is read into: the grouping columns followed by the aggregate. Everything is
     * declared {@code LONG} because that is what a tuple holds, keeping the experiment about placement rather than about
     * coercion.
     */
    private static final Type.Record TARGET_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of(GROUP_1)),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of(GROUP_2)),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of(AGGREGATE))));

    private static final int GROUPING_COUNT = 2;
    private static final int GROUPED_COUNT = 1;

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    private KeySpacePath path;
    private FDBRecordStore recordStore;

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    /**
     * A plain aggregate index keeps the grouping columns in the entry key and the aggregate in the entry value, so the
     * value reads from both tuples.
     */
    @Test
    void readsAnAggregateEntryIntoARecord() {
        withRecord(() -> {
            final IndexEntry entry = scanByGroup(MAX_EVER_INDEX);
            assertEquals(Tuple.from(10, 20), entry.getKey());
            assertEquals(Tuple.from(100), entry.getValue());

            final var recordValue = AggregateIndexMatchCandidate.indexEntryToRecordValue(TARGET_TYPE,
                    baseObjectValue(), GROUPING_COUNT, GROUPED_COUNT, false, 0);
            assertEquals(List.of(10L, 20L, 100L), fieldsOf(evaluate(recordValue, entry)));
        });
    }

    /**
     * A permuted index keeps everything in the entry key, with the trailing grouping column moved behind the aggregate,
     * so the very same target record is read from a different arrangement.
     */
    @Test
    void readsAPermutedEntryIntoTheSameRecord() {
        withRecord(() -> {
            final IndexEntry entry = scanByGroup(PERMUTED_INDEX);
            assertEquals(Tuple.from(10, 100, 20), entry.getKey());
            assertEquals(Tuple.from(), entry.getValue());

            final var recordValue = AggregateIndexMatchCandidate.indexEntryToRecordValue(TARGET_TYPE,
                    baseObjectValue(), GROUPING_COUNT, GROUPED_COUNT, true, 1);
            assertEquals(List.of(10L, 20L, 100L), fieldsOf(evaluate(recordValue, entry)));
        });
    }


    /**
     * The value has to survive a round trip through its serialized form, because a plan holding one is serialized into
     * continuations. It does not: writing it loses whether its result type is nullable, and reading it back verifies
     * that the field is present.
     * <p>
     * Needs no record store despite this class being tagged as requiring one -- it is only a value being written and
     * read.
     * </p>
     */
    @Test
    void theValueSurvivesSerialization() {
        final var recordValue = AggregateIndexMatchCandidate.indexEntryToRecordValue(TARGET_TYPE, baseObjectValue(),
                GROUPING_COUNT, GROUPED_COUNT, false, 0);

        final var proto = recordValue.toValueProto(PlanSerializationContext.newForCurrentMode());
        final var roundTripped = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(), proto);

        assertEquals(recordValue, roundTripped);
    }



    /**
     * A permuted index not ordered by its aggregate has a permuted size of zero, yet still holds every column in the
     * entry key. Reading the aggregate from the entry value would find nothing there, which is what the SQL layer creates
     * for a plain grouped {@code max(...)}.
     */
    @Test
    void permutedIndexOfSizeZeroKeepsEverythingInTheKey() {
        final var recordValue = AggregateIndexMatchCandidate.indexEntryToRecordValue(TARGET_TYPE, baseObjectValue(),
                GROUPING_COUNT, GROUPED_COUNT, true, 0);

        assertEquals(List.of("KEY:[0]", "KEY:[1]", "KEY:[2]"), sourcesOf(recordValue));
    }

    @Nonnull
    private static QuantifiedObjectValue baseObjectValue() {
        return QuantifiedObjectValue.of(Quantifier.current(), TARGET_TYPE);
    }

    /**
     * Where each column of the given value reads from, as {@code SOURCE[ordinal]}, in target field order.
     */
    /**
     * How each column of the given value renders, in target field order, which says where it reads from. This is what
     * the copiers of an {@code IndexKeyValueToPartialRecord} would otherwise be holding.
     */
    @Nonnull
    private static List<String> sourcesOf(@Nonnull final RecordConstructorValue recordValue) {
        return recordValue.getColumns().stream()
                .map(column -> column.getValue().toString())
                .collect(Collectors.toList());
    }

    @Nonnull
    private static Message evaluate(@Nonnull final RecordConstructorValue recordValue, @Nonnull final IndexEntry entry) {
        final var typeRepository = TypeRepository.newBuilder().addTypeIfNeeded(recordValue.getResultType()).build();
        final var evaluationContext = EvaluationContext.forBindingsAndTypeRepository(
                Bindings.newBuilder()
                        .set(Bindings.Internal.CORRELATION.bindingName(Quantifier.current().getId()), entry)
                        .build(),
                typeRepository);
        return (Message)recordValue.evalWithoutStore(evaluationContext);
    }

    @Nonnull
    private static List<Object> fieldsOf(@Nonnull final Message message) {
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        return List.of(message.getField(descriptor.findFieldByName(GROUP_1)),
                message.getField(descriptor.findFieldByName(GROUP_2)),
                message.getField(descriptor.findFieldByName(AGGREGATE)));
    }

    @Nonnull
    private IndexEntry scanByGroup(@Nonnull final String indexName) {
        final var entries = recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(indexName),
                new IndexScanRange(IndexScanType.BY_GROUP, TupleRange.ALL), null,
                ScanProperties.FORWARD_SCAN).asList().join();
        assertEquals(1, entries.size(), "the fixture saves exactly one record, so there is one group");
        return entries.get(0);
    }

    @Nonnull
    private static RecordMetaData metaData() {
        final RecordMetaDataBuilder metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final var groupedByTwo = field("num_value_unique")
                .groupBy(concat(field("num_value_2"), field("num_value_3_indexed")));
        metaDataBuilder.addIndex("MySimpleRecord", new Index(MAX_EVER_INDEX, groupedByTwo, IndexTypes.MAX_EVER_LONG));
        metaDataBuilder.addIndex("MySimpleRecord", new Index(PERMUTED_INDEX, groupedByTwo,
                IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1")));
        return metaDataBuilder.build();
    }

    private void withRecord(@Nonnull final Runnable body) {
        try (FDBRecordContext context = fdb.openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setMetaDataProvider(metaData())
                    .setContext(context)
                    .setKeySpacePath(path)
                    .createOrOpen();
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setNumValue2(10)
                    .setNumValue3Indexed(20)
                    .setNumValueUnique(100)
                    .build());
            body.run();
            context.commit();
        }
    }
}
