/*
 * SchemaTemplateSerDeTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataDeserializer;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.test.BooleanSource;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains a number of tests for serializing and deserializing {@link RecordLayerSchemaTemplate}.
 */
public class SchemaTemplateSerDeTests {

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    @Nonnull
    private static RecordLayerSchemaTemplate basicTestTemplate() {
        return RecordLayerSchemaTemplate.newBuilder().setName("TestSchemaTemplate")
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("t1")
                        .addColumn(
                                RecordLayerColumn
                                        .newBuilder()
                                        .setName("col1")
                                        .setDataType(DataType.Primitives.INTEGER.type())
                                        .build())
                        .addIndex(
                                RecordLayerIndex
                                        .newBuilder()
                                        .setName("i1")
                                        .setTableName("t1")
                                        .setIndexType(IndexTypes.VALUE)
                                        .setKeyExpression(Key.Expressions.field("col1", KeyExpression.FanType.None))
                                        .build())
                        .addIndex(
                                RecordLayerIndex
                                        .newBuilder()
                                        .setName("i2")
                                        .setTableName("t1")
                                        .setIndexType(IndexTypes.VALUE)
                                        .setKeyExpression(Key.Expressions.field("col1", KeyExpression.FanType.None))
                                        .build())
                        .addIndex(
                                RecordLayerIndex
                                        .newBuilder()
                                        .setName("i3")
                                        .setTableName("t1")
                                        .setIndexType(IndexTypes.VALUE)
                                        .setKeyExpression(Key.Expressions.field("col1", KeyExpression.FanType.None))
                                        .build())
                        .addIndex(
                                RecordLayerIndex
                                        .newBuilder()
                                        .setName("i4")
                                        .setTableName("t1")
                                        .setIndexType(IndexTypes.VALUE)
                                        .setKeyExpression(Key.Expressions.field("col1", KeyExpression.FanType.None))
                                        .build())
                        .build())
                .build();
    }

    private static RecordLayerSchemaTemplate getTestRecordLayerSchemaTemplate(@Nonnull Map<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>> template) {
        final var builder = RecordLayerSchemaTemplate.newBuilder().setName("TestSchemaTemplate");
        for (var entry : template.entrySet()) {
            final var tableBuilder = RecordLayerTable.newBuilder(false)
                    .setName(entry.getKey())
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setName(entry.getKey() + "_C")
                            .setDataType(DataType.Primitives.STRING.type())
                            .build());
            for (var generation : entry.getValue()) {
                tableBuilder.addGeneration(generation.getLeft(), generation.getRight());
            }
            builder.addTable(tableBuilder.build());
        }
        return builder.build();
    }

    @Test
    void testGoodSchemaTemplate() {
        var testcase = new HashMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase.put("T1", List.of());
        testcase.put("T2", List.of());

        var template = getTestRecordLayerSchemaTemplate(testcase);
        var recordMetadataProto = template.toRecordMetadata().toProto();

        final var maybeUnionDesc = recordMetadataProto.getRecords().getMessageTypeList().stream()
                .filter(m -> "RecordTypeUnion".equals(m.getName()))
                .findFirst();
        Assertions.assertTrue(maybeUnionDesc.isPresent());
        final var unionDesc = maybeUnionDesc.get();

        // Check if all tables are part of union descriptor.
        final var expectedTableNameSet = Set.of("T1", "T2");
        Assertions.assertTrue(unionDesc.getFieldList().stream().allMatch(e -> expectedTableNameSet.contains(e.getTypeName())));

        // Check if the number of fields in union descriptor are equal to the tables in the template.
        final var expectedNumUnionFields = testcase.size();
        Assertions.assertEquals(expectedNumUnionFields, unionDesc.getFieldList().size());

        // Check if field numbers are assigned sequentially from [1, n]
        final var actualFieldNumbers = new HashSet<>();
        unionDesc.getFieldList().forEach(e -> actualFieldNumbers.add(e.getNumber()));
        for (var fieldNumber = 1; fieldNumber <= expectedNumUnionFields; fieldNumber++) {
            Assertions.assertTrue(actualFieldNumbers.contains(fieldNumber));
        }
    }

    @ParameterizedTest(name = "testEnableLongRows[enableLongRows-{0}]")
    @ValueSource(booleans = {false, true})
    void testEnableLongRows(boolean enableLongRows) {
        RecordLayerSchemaTemplate schemaTemplate = basicTestTemplate().toBuilder()
                .setVersion(42)
                .setEnableLongRows(enableLongRows)
                .build();
        Assertions.assertEquals(enableLongRows, schemaTemplate.isEnableLongRows());

        // Validate the schema template option is included in the final meta-data
        RecordMetaData metaData = schemaTemplate.toRecordMetadata();
        Assertions.assertEquals(enableLongRows, metaData.isSplitLongRecords());

        // Validate that when wrapping a met
        RecordLayerSchemaTemplate wrappedMetaData = RecordLayerSchemaTemplate.fromRecordMetadata(metaData, schemaTemplate.getName(), schemaTemplate.getVersion());
        Assertions.assertEquals(enableLongRows, wrappedMetaData.isEnableLongRows());
        Assertions.assertEquals(schemaTemplate.getVersion(), wrappedMetaData.getVersion());
    }

    @ParameterizedTest(name = "testStoreRowVersions[storeRowVersions-{0}]")
    @ValueSource(booleans = {false, true})
    void testStoreRowVersions(boolean storeRowVersions) {
        RecordLayerSchemaTemplate schemaTemplate = basicTestTemplate().toBuilder()
                .setVersion(42)
                .setStoreRowVersions(storeRowVersions)
                .build();
        Assertions.assertEquals(storeRowVersions, schemaTemplate.isStoreRowVersions());
        Assertions.assertEquals(storeRowVersions, schemaTemplate.toRecordMetadata().isStoreRecordVersions());

        RecordMetaData metaData = schemaTemplate.toRecordMetadata();
        RecordLayerSchemaTemplate wrappedMetaData = RecordLayerSchemaTemplate.fromRecordMetadata(metaData, schemaTemplate.getName(), schemaTemplate.getVersion());
        Assertions.assertEquals(storeRowVersions, wrappedMetaData.isStoreRowVersions());
        Assertions.assertEquals(schemaTemplate.getVersion(), wrappedMetaData.getVersion());
    }

    @Test
    void testGoodSchemaTemplateWithGenerations() {
        final var fieldOptions1 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(true).build();
        final var fieldOptions2 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(false).build();
        var testcase = new HashMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase.put("T1", List.of(NonnullPair.of(1, fieldOptions1), NonnullPair.of(2, fieldOptions2)));
        testcase.put("T2", List.of(NonnullPair.of(3, fieldOptions2), NonnullPair.of(4, fieldOptions1)));

        var template = getTestRecordLayerSchemaTemplate(testcase);
        var recordMetadataProto = template.toRecordMetadata().toProto();

        final var maybeUnionDesc = recordMetadataProto.getRecords().getMessageTypeList().stream()
                .filter(m -> "RecordTypeUnion".equals(m.getName()))
                .findFirst();
        Assertions.assertTrue(maybeUnionDesc.isPresent());
        final var unionDesc = maybeUnionDesc.get();

        // Check if the number of fields in union descriptor are equal to total number of generations across all tables.
        final var expectedUnionFields = testcase.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(expectedUnionFields, unionDesc.getFieldList().size());

        // Check if all generations are present in union descriptor
        for (final var unionField : unionDesc.getFieldList()) {
            final var typeName = unionField.getTypeName();
            Assertions.assertTrue(testcase.containsKey(typeName));
            final var expectedGenerations = testcase.get(typeName);
            Assertions.assertTrue(expectedGenerations.contains(NonnullPair.of(unionField.getNumber(), unionField.getOptions())));
        }
    }

    @Test
    void readableIndexBitsetWorksCorrectly() throws RelationalException {
        final var template = basicTestTemplate();
        // we have table "t1" with four indexes "i1, i2, i3, i4".
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000001}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i1"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000010}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i2"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000100}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i3"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00001000}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i4"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000110}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i2", "i3"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000110}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i3", "i2"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00000101}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i1", "i3"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00001110}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i4", "i2", "i3"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00001110}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i2", "i4", "i3"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00001110}), template.getIndexEntriesAsBitset(Optional.of(Set.of("i2", "i3", "i4"))));
        Assertions.assertEquals(BitSet.valueOf(new long[]{0b00001111}), template.getIndexEntriesAsBitset(Optional.empty()));
    }

    @Nonnull
    static Stream<Arguments> badSchemaTemplateGenerationsTestcaseProvider() {
        final var fieldOptions1 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(true).build();
        final var fieldOptions2 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(false).build();

        // SchemaTemplate with field number 0
        var testcase1 = new TreeMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase1.put("T1", List.of(NonnullPair.of(0, fieldOptions1), NonnullPair.of(2, fieldOptions2)));
        // SchemaTemplate with duplicated field number
        var testcase2 = new TreeMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase2.put("T1", List.of(NonnullPair.of(1, fieldOptions1), NonnullPair.of(1, fieldOptions2)));
        // SchemaTemplate with duplicated fieldOptions
        var testcase3 = new TreeMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase3.put("T1", List.of(NonnullPair.of(1, fieldOptions2), NonnullPair.of(2, fieldOptions2)));
        // SchemaTemplate with duplicated field numbers across tables
        var testcase4 = new TreeMap<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase4.put("T1", List.of(NonnullPair.of(1, fieldOptions2), NonnullPair.of(2, fieldOptions1)));
        testcase4.put("T2", List.of(NonnullPair.of(2, fieldOptions2), NonnullPair.of(3, fieldOptions1)));

        return Stream.of(
                Arguments.of(testcase1, UncheckedRelationalException.class, "Field numbers must be positive integers"),
                Arguments.of(testcase2, UncheckedRelationalException.class, "Duplicate field number 1 for generation of Table T1"),
                Arguments.of(testcase3, UncheckedRelationalException.class, "Duplicated options for different generations of Table T1"),
                Arguments.of(testcase4, UncheckedRelationalException.class, "Field number 2 has already been used")
        );
    }

    @ParameterizedTest
    @MethodSource("badSchemaTemplateGenerationsTestcaseProvider")
    void testBadSchemaTemplateGenerations(Map<String, List<NonnullPair<Integer, DescriptorProtos.FieldOptions>>> testcase,
                                                 Class<? extends Exception> exceptionClass, String message) {
        final var thrown = Assertions.assertThrows(exceptionClass, () -> {
            final var schemaTemplate = getTestRecordLayerSchemaTemplate(testcase);
            schemaTemplate.toRecordMetadata();
        });
        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString(message));
    }

    @Test
    void deserializationNestedTypesPreservesNamesCorrectly() {
        final var sampleRecordSchemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addAuxiliaryType(DataType.StructType.from(
                        "Subtype",
                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 0)),
                        true))
                .addTable(
                        RecordLayerTable.newBuilder(false)
                                .setName("T1")
                                .addColumn(RecordLayerColumn.newBuilder()
                                        .setName("COL1")
                                        .setDataType(
                                                DataType.StructType.from(
                                                        "Subtype",
                                                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                                                        true))
                                        .build())
                                .build())
                .build();
        final var proto = sampleRecordSchemaTemplate.toRecordMetadata();
        final var deserializedTableType = RecordLayerSchemaTemplate.fromRecordMetadata(proto, "TestSchemaTemplate", 42).findTableByName("T1");
        Assertions.assertTrue(deserializedTableType.isPresent());
        final var column = deserializedTableType.get().getColumns().stream().findFirst();
        Assertions.assertTrue(column.isPresent());
        final var type = column.get().getDataType();
        Assertions.assertInstanceOf(DataType.StructType.class, type);
        final var typeName = ((DataType.StructType) type).getName();
        Assertions.assertEquals("Subtype", typeName);
    }


    @Test
    void deserializationTranslatesUserDefinedNameCorrectly() {
        final var metaDataBuilder = RecordMetaData.newBuilder();
        metaDataBuilder.setRecords(createEscapedRecordTypesDescriptor());
        RecordTypeBuilder typeBuilder = metaDataBuilder.getRecordType("Foo__0Bar__1Baz__2End");
        final var primaryKey = Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("id"));
        typeBuilder.setPrimaryKey(primaryKey);
        typeBuilder.setRecordTypeKey(1L);
        metaDataBuilder.addIndex(typeBuilder, new Index("Foo__Bar$Baz.End$$a__b$c.d", "a__0b__1c__2d"));
        final RecordMetaData metaData = metaDataBuilder.build();
        final var actualSchemaTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(metaData, "TestSchemaTemplate", metaData.getVersion());

        // the RecordLayerSchemaTemplate deserializer translates proto fields to user-defined names.
        final var expectedTableName = "Foo__Bar$Baz.End";
        Assertions.assertEquals(1, actualSchemaTemplate.getTables().size());
        final var tableMaybe = actualSchemaTemplate.findTableByName(expectedTableName);
        Assertions.assertTrue(tableMaybe.isPresent());
        final var actualTable = tableMaybe.get();
        final var actualRecordTable = Assertions.assertInstanceOf(RecordLayerTable.class, actualTable);
        Assertions.assertEquals("Foo__0Bar__1Baz__2End", actualRecordTable.getType().getStorageName());

        final var expectedTable = RecordLayerTable.newBuilder(false)
                .setName(expectedTableName)
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("a__b$c.d")
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("otherField")
                        .setDataType(DataType.Primitives.NULLABLE_STRING.type())
                        .build())
                .setPrimaryKey(primaryKey)
                .build();
        Assertions.assertEquals(expectedTable.getName(), actualRecordTable.getName());
        Assertions.assertEquals(expectedTable.getColumns(), actualRecordTable.getColumns());
        Assertions.assertEquals(expectedTable.getPrimaryKey(), actualRecordTable.getPrimaryKey());

        final var actualIndexes = actualTable.getIndexes();
        Assertions.assertEquals(1, actualIndexes.size(), () -> "actual indexes: " + actualIndexes + " should have size 1");
        final var actualIndex = Iterables.getOnlyElement(actualIndexes);
        Assertions.assertEquals("Foo__Bar$Baz.End$$a__b$c.d", actualIndex.getName());
        Assertions.assertEquals(expectedTableName, actualIndex.getTableName());
        Assertions.assertInstanceOf(RecordLayerIndex.class, actualIndex);
        final var actualRecordLayerIndex = (RecordLayerIndex) actualIndex;
        Assertions.assertEquals(Key.Expressions.field("a__0b__1c__2d"), actualRecordLayerIndex.getKeyExpression());
        Assertions.assertEquals("Foo__0Bar__1Baz__2End", actualRecordLayerIndex.getTableStorageName());
    }

    @Test
    void deserializeTemplateWithMalformedNamesCorrectly() {
        final var metaDataBuilder = RecordMetaData.newBuilder();
        metaDataBuilder.setRecords(createRecordTypesDescriptorWithMalformedEscaping());
        RecordTypeBuilder typeBuilder = metaDataBuilder.getRecordType("_Foo__Bar__1Baz");
        final var primaryKey = Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("id"));
        typeBuilder.setPrimaryKey(primaryKey);
        typeBuilder.setRecordTypeKey(1L);
        metaDataBuilder.addIndex(typeBuilder, new Index("_Foo__Bar__1Baz$a__b$c.d", "a__b__1c__2d"));
        final RecordMetaData metaData = metaDataBuilder.build();
        final var actualSchemaTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(metaData, "TestSchemaTemplate", metaData.getVersion());

        // the RecordLayerSchemaTemplate deserializer translates proto fields to user-defined names as best it can.
        // Note that if we went back the other way, the table name we'd have gotten back would have a type name of
        // "Foo__0Bar__1Baz"
        final var expectedTableName = "_Foo__Bar$Baz";
        Assertions.assertEquals(1, actualSchemaTemplate.getTables().size());
        final var tableMaybe = actualSchemaTemplate.findTableByName(expectedTableName);
        Assertions.assertTrue(tableMaybe.isPresent());
        final var actualTable = tableMaybe.get();
        final var actualRecordTable = Assertions.assertInstanceOf(RecordLayerTable.class, actualTable);
        Assertions.assertEquals("_Foo__Bar__1Baz", actualRecordTable.getType().getStorageName());

        final var expectedTable = RecordLayerTable.newBuilder(false)
                .setName(expectedTableName)
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("a__b$c.d")
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("otherField")
                        .setDataType(DataType.Primitives.NULLABLE_STRING.type())
                        .build())
                .setPrimaryKey(primaryKey)
                .build();
        Assertions.assertEquals(expectedTable.getName(), actualRecordTable.getName());
        Assertions.assertEquals(expectedTable.getColumns(), actualRecordTable.getColumns());
        Assertions.assertEquals(expectedTable.getPrimaryKey(), actualRecordTable.getPrimaryKey());

        final var actualIndexes = actualTable.getIndexes();
        Assertions.assertEquals(1, actualIndexes.size(), () -> "actual indexes: " + actualIndexes + " should have size 1");
        final var actualIndex = Iterables.getOnlyElement(actualIndexes);
        Assertions.assertEquals("_Foo__Bar__1Baz$a__b$c.d", actualIndex.getName());
        Assertions.assertEquals(expectedTableName, actualIndex.getTableName());
        Assertions.assertInstanceOf(RecordLayerIndex.class, actualIndex);
        final var actualRecordLayerIndex = (RecordLayerIndex) actualIndex;
        Assertions.assertEquals(Key.Expressions.field("a__b__1c__2d"), actualRecordLayerIndex.getKeyExpression());
        Assertions.assertEquals("_Foo__Bar__1Baz", actualRecordLayerIndex.getTableStorageName());
    }

    @Test
    void findTableByNameWorksCorrectly() {
        final var sampleRecordSchemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addAuxiliaryType(DataType.StructType.from(
                        "Subtype",
                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 0)),
                        true))
                .addTable(
                        RecordLayerTable.newBuilder(false)
                                .setName("T1")
                                .addColumn(RecordLayerColumn.newBuilder()
                                        .setName("COL1")
                                        .setDataType(
                                                DataType.StructType.from(
                                                        "Subtype",
                                                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                                                        true))
                                        .build())
                                .build())
                .build();
        final var foundTableMaybe = sampleRecordSchemaTemplate.findTableByName("T1");
        Assertions.assertTrue(foundTableMaybe.isPresent());
        Assertions.assertEquals("T1", foundTableMaybe.get().getName());
        Assertions.assertDoesNotThrow(() -> sampleRecordSchemaTemplate.findTableByName("BLA"));
        final var nonExisting = sampleRecordSchemaTemplate.findTableByName("BLA");
        Assertions.assertFalse(nonExisting.isPresent());
    }

    @Test
    void sqlFunctionsAreLazilyParsed() throws Exception {
        final var peekingDeserializer = recMetadataSampleWithFunctions(
                "CREATE FUNCTION SqlFunction1(IN Q BIGINT) AS SELECT * FROM T1 WHERE COL1 < Q");
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction1"));

        final var planGenerator = peekingDeserializer.getPlanGenerator();
        var plan = planGenerator.getPlan("select * from SqlFunction1(100)");
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertNotNull(plan);

        plan = planGenerator.getPlan("select * from SqlFunction1(200)");
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertNotNull(plan);
    }

    @Test
    void nestedSqlFunctionsAreLazilyParsed() throws Exception {
        final var peekingDeserializer = recMetadataSampleWithFunctions(
                "CREATE FUNCTION SqlFunction1(IN Q BIGINT) AS SELECT * FROM T1 WHERE COL1 < Q",
                "CREATE FUNCTION SqlFunction2(IN Q BIGINT) AS SELECT * FROM SqlFunction1(100) WHERE COL1 < Q");
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction2"));

        final var planGenerator = peekingDeserializer.getPlanGenerator();
        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction1(100)"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction2"));

        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction2(200)"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction2"));

        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction2(200) where COL1 < 300"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction2"));
    }

    @Test
    void onlyQueriedSqlFunctionsAreCompiled() throws Exception {
        final var peekingDeserializer = recMetadataSampleWithFunctions(
                "CREATE FUNCTION SqlFunction1(IN Q BIGINT) AS SELECT * FROM T1 WHERE COL1 < Q",
                "CREATE FUNCTION SqlFunction2(IN Q BIGINT) AS SELECT * FROM SqlFunction1(100) WHERE COL1 < Q",
                "CREATE FUNCTION SqlFunction3() AS SELECT * FROM T1");
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction2"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction3"));

        final var planGenerator = peekingDeserializer.getPlanGenerator();
        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction1(100)"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction2"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction3"));

        planGenerator.getPlan("select * from SqlFunction2(200)");

        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction2(200)"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction2"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction3"));

        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction2(200) where COL1 < 300"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction2"));
        Assertions.assertTrue(peekingDeserializer.hasNoCompilationRequestsFor("SqlFunction4"));

        Assertions.assertDoesNotThrow(() -> planGenerator.getPlan("select * from SqlFunction3() where COL1 < 300"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction1"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction2"));
        Assertions.assertTrue(peekingDeserializer.hasOneCompilationRequestFor("SqlFunction3"));
    }

    @ParameterizedTest(name = "schema template builder preserving intermingledTables flag set to {0}")
    @ValueSource(booleans = {true, false})
    void schemaTemplateToBuilderPreservesIntermingledTablesFlag(boolean intermingleTables) {
        var sampleRecordSchemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addAuxiliaryType(DataType.StructType.from(
                        "Subtype",
                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 0)),
                        true))
                .setIntermingleTables(intermingleTables)
                .addTable(
                        RecordLayerTable.newBuilder(intermingleTables)
                                .setName("T1")
                                .addColumn(RecordLayerColumn.newBuilder()
                                        .setName("COL1")
                                        .setDataType(
                                                DataType.StructType.from(
                                                        "Subtype",
                                                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                                                        true))
                                        .build())
                                .build())
                .build();

        // make sure the intermingleTables flag is preserved after creating the invoked routine in the builder
        // as well as in the built schema template.
        var builder = sampleRecordSchemaTemplate.toBuilder();
        Assertions.assertEquals(intermingleTables, builder.isIntermingleTables());
        sampleRecordSchemaTemplate = builder.build();
        Assertions.assertEquals(intermingleTables, sampleRecordSchemaTemplate.isIntermingleTables());

        final var funcName = "SqlFunction1";
        final var funcDescription = "CREATE FUNCTION SqlFunction1(IN Q BIGINT) AS SELECT * FROM T1 WHERE col1 < Q";
        // add temporary invoked routine.
        builder.addInvokedRoutine(RecordLayerInvokedRoutine.newBuilder()
                .setName(funcName)
                .setDescription(funcDescription)
                .setTemporary(true)
                .withUserDefinedFunctionProvider(ignored -> new CompiledFunctionStub())
                .withSerializableFunction(new RawSqlFunction(funcName, funcDescription))
                .build());

        // build the schema template
        final var newSchemaTemplate = builder.build();

        // make sure the intermingleTables flag is preserved after creating the invoked routine in the builder
        // as well as the built schema template.
        builder = newSchemaTemplate.toBuilder();
        Assertions.assertEquals(intermingleTables, builder.isIntermingleTables());
        sampleRecordSchemaTemplate = builder.build();
        Assertions.assertEquals(intermingleTables, sampleRecordSchemaTemplate.isIntermingleTables());
    }

    @Nonnull
    private static RecordMetadataDeserializerWithPeekingFunctionSupplier recMetadataSampleWithFunctions(@Nonnull final String... functions) {
        final var schemaTemplateBuilder = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addTable(
                        RecordLayerTable.newBuilder(false)
                                .setName("T1")
                                .addColumn(RecordLayerColumn.newBuilder()
                                        .setName("COL1")
                                        .setDataType(DataType.Primitives.INTEGER.type())
                                        .build())
                                .build());
        final var pattern = Pattern.compile("CREATE FUNCTION (\\w+)\\(");
        final var expectedFunctionMapBuilder = ImmutableMap.<String, String>builder();
        for (final var function : functions) {
            Matcher matcher = pattern.matcher(function);
            Assert.thatUnchecked(matcher.find());
            final var functionName = matcher.group(1);
            expectedFunctionMapBuilder.put(functionName, function);
        }

        final var expectedFunctionMap = expectedFunctionMapBuilder.build();
        for (final var entry : expectedFunctionMap.entrySet()) {
            final var functionName = entry.getKey();
            final var functionDescription = entry.getValue();
            schemaTemplateBuilder.addInvokedRoutine(RecordLayerInvokedRoutine.newBuilder()
                    .setName(functionName)
                    .setDescription(functionDescription)
                    .withUserDefinedFunctionProvider(igored -> new CompiledFunctionStub())
                    .withSerializableFunction(new RawSqlFunction(functionName, functionDescription))
                    .build());
        }

        final var recordMetadata = schemaTemplateBuilder.build().toRecordMetadata();
        final var invokedRoutines = recordMetadata.getUserDefinedFunctionMap();
        final var actualFunctionMap = invokedRoutines.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, e -> ((RawSqlFunction)e.getValue()).getDefinition()));

        // Verify that the provided functions match the ones we just deserialized
        Assertions.assertEquals(expectedFunctionMap, actualFunctionMap);
        for (final var entry : expectedFunctionMap.entrySet()) {
            final var functionName = entry.getKey();
            final var functionDescription = entry.getValue();
            Assertions.assertTrue(invokedRoutines.containsKey(functionName));
            final var function = invokedRoutines.get(functionName);
            Assertions.assertInstanceOf(RawSqlFunction.class, function);
            final var rawSqlFunction = (RawSqlFunction)function;
            Assertions.assertEquals(functionName, rawSqlFunction.getFunctionName());
            Assertions.assertEquals(functionDescription, rawSqlFunction.getDefinition());
        }

        // let's verify now that _no_ compilation is invoked when deserializing the record metadata.
        // for that, we use a deserializer with peeking supplier to the function compilation logic.
        final var deserializerWithPeekingCompilationSupplier = new RecordMetadataDeserializerWithPeekingFunctionSupplier(recordMetadata);
        for (final var functionName : expectedFunctionMap.keySet()) {
            Assertions.assertTrue(deserializerWithPeekingCompilationSupplier.hasNoCompilationRequestsFor(functionName));
        }
        deserializerWithPeekingCompilationSupplier.getSchemaTemplate("schemaUnderTest", 42);
        for (final var functionName : expectedFunctionMap.keySet()) {
            Assertions.assertTrue(deserializerWithPeekingCompilationSupplier.hasNoCompilationRequestsFor(functionName));
        }
        return deserializerWithPeekingCompilationSupplier;
    }

    private static final class CompiledFunctionStub extends CompiledSqlFunction {
        @SuppressWarnings("DataFlowIssue") // only for test.
        CompiledFunctionStub() {
            super("something", List.of(), List.of(), List.of(),
                    Optional.empty(), null, Literals.empty());
        }
    }

    @Test
    void testViewCreationInSchemaTemplate() {
        // Create a schema template with a table and a view
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("salary")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("high_salary_view")
                        .setDescription("SELECT * FROM employees WHERE salary > 50000")
                        .setViewCompiler(ignored -> null)  // Stub for now, view expansion not implemented
                        .build())
                .build();

        // Verify the view was added
        Assertions.assertEquals(1, schemaTemplate.getViews().size());
        final var viewOpt = schemaTemplate.findViewByName("high_salary_view");
        Assertions.assertTrue(viewOpt.isPresent());
        Assertions.assertEquals("high_salary_view", viewOpt.get().getName());
        Assertions.assertEquals("SELECT * FROM employees WHERE salary > 50000", viewOpt.get().getDescription());
    }

    @Test
    void testMultipleViewsInSchemaTemplate() {
        // Create a schema template with multiple views
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("view1")
                        .setDescription("SELECT * FROM employees")
                        .setViewCompiler(ignored -> null)
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("view2")
                        .setDescription("SELECT id FROM employees")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        // Verify both views exist
        Assertions.assertEquals(2, schemaTemplate.getViews().size());
        Assertions.assertTrue(schemaTemplate.findViewByName("view1").isPresent());
        Assertions.assertTrue(schemaTemplate.findViewByName("view2").isPresent());
    }

    @Test
    void testReplaceViewInSchemaTemplate() {
        // Create initial schema template with a view
        final var initialTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("test_view")
                        .setDescription("SELECT * FROM employees WHERE id > 10")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        // Replace the view with a new definition
        final var updatedTemplate = initialTemplate.toBuilder()
                .replaceView(RecordLayerView.newBuilder()
                        .setName("test_view")
                        .setDescription("SELECT * FROM employees WHERE id > 100")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        // Verify the view was replaced
        Assertions.assertEquals(1, updatedTemplate.getViews().size());
        final var viewOpt = updatedTemplate.findViewByName("test_view");
        Assertions.assertTrue(viewOpt.isPresent());
        Assertions.assertEquals("SELECT * FROM employees WHERE id > 100", viewOpt.get().getDescription());
    }

    @Test
    void testRemoveViewFromSchemaTemplate() {
        // Create schema template with a view
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("test_view")
                        .setDescription("SELECT * FROM employees")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        Assertions.assertEquals(1, schemaTemplate.getViews().size());

        // Remove the view
        final var updatedTemplate = schemaTemplate.toBuilder()
                .removeView("test_view")
                .build();

        // Verify the view was removed
        Assertions.assertEquals(0, updatedTemplate.getViews().size());
        Assertions.assertFalse(updatedTemplate.findViewByName("test_view").isPresent());
    }

    /**
     * Navigates to an array's elements, the way the DDL layer does: a nullable array is stored wrapped as
     * {@code { repeated T values; }}, a non-nullable one is a plain repeated field.
     */
    @Nonnull
    private static KeyExpression arrayElementsExpression(final String arrayFieldName, final boolean nullableArray) {
        return nullableArray
               ? Key.Expressions.field(arrayFieldName)
                       .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut))
               : Key.Expressions.field(arrayFieldName, KeyExpression.FanType.FanOut);
    }

    @Nonnull
    private static KeyExpression constituentField(final String alias, final String fieldName) {
        return Key.Expressions.field(alias, KeyExpression.FanType.None).nest(fieldName);
    }

    @Nonnull
    private static DataType.StructType struct(final String name, final DataType.StructType.Field... fields) {
        return DataType.StructType.from(name, List.of(fields), false);
    }

    @Nonnull
    private static DataType.StructType.Field structField(final String name, final DataType type, final int number) {
        return DataType.StructType.Field.from(name, type, number);
    }

    /** A table with a {@code bigint id} primary key plus the one column the synthetic type unnests through. */
    @Nonnull
    private static RecordLayerTable tableWithId(final String tableName, final String columnName,
                                               final DataType columnType) {
        return RecordLayerTable.newBuilder(false)
                .setName(tableName)
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Primitives.LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName(columnName)
                        .setDataType(columnType)
                        .build())
                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("id")))
                .build();
    }

    @Nonnull
    private static RecordLayerUnnestedSyntheticTable syntheticTable(
            final String syntheticName, final RecordLayerTable parentTable, final String indexName,
            final KeyExpression keyExpression,
            final RecordLayerUnnestedSyntheticTable.NestedConstituent... constituents) {
        final var builder = RecordLayerUnnestedSyntheticTable.newBuilder()
                .setName(syntheticName)
                .setAlias("row")
                .setParentTableType(parentTable.getType());
        for (final var constituent : constituents) {
            builder.addConstituent(constituent);
        }
        return builder.addIndex(RecordLayerIndex.newBuilder()
                        .setName(indexName)
                        .setTableName(syntheticName)
                        .setTableStorageName(syntheticName)
                        .setIndexType(IndexTypes.VALUE)
                        .setKeyExpression(keyExpression)
                        .build())
                .build();
    }

    @Nonnull
    private static RecordLayerSchemaTemplate templateWith(final RecordLayerTable table,
                                                          final RecordLayerUnnestedSyntheticTable syntheticTable,
                                                          final DataType.Named... auxiliaryTypes) {
        final var builder = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42);
        for (final var auxiliaryType : auxiliaryTypes) {
            builder.addAuxiliaryType(auxiliaryType);
        }
        return builder.addTable(table).addSyntheticTable(syntheticTable).build();
    }

    /** Serializes, asserting the synthetic type reaches the metadata rather than being silently dropped. */
    @Nonnull
    private static RecordMetaData serializeWithSyntheticType(final RecordLayerSchemaTemplate template,
                                                             final String syntheticName) {
        final var recordMetaData = template.toRecordMetadata();
        Assertions.assertTrue(recordMetaData.getSyntheticRecordTypes().containsKey(syntheticName),
                () -> "synthetic type missing from serialized metadata, got "
                        + recordMetaData.getSyntheticRecordTypes().keySet());
        return recordMetaData;
    }

    /** The sole non-parent constituent of a serialized synthetic type. */
    @Nonnull
    private static UnnestedRecordType.NestedConstituent serializedConstituent(final RecordMetaData recordMetaData,
                                                                             final String syntheticName) {
        final var unnestedRecordType = (UnnestedRecordType) recordMetaData.getSyntheticRecordTypes().get(syntheticName);
        return unnestedRecordType.getConstituents().stream()
                .filter(candidate -> !candidate.isParent()).findFirst().orElseThrow();
    }

    @Nonnull
    private static RecordLayerUnnestedSyntheticTable deserializeSyntheticTable(final RecordMetaData recordMetaData) {
        final var syntheticTables = RecordLayerSchemaTemplate
                .fromRecordMetadata(recordMetaData, "TestSchemaTemplate", 42)
                .getUnnestedSyntheticTables();
        Assertions.assertEquals(1, syntheticTables.size());
        return syntheticTables.stream().findFirst().orElseThrow();
    }

    /**
     * Round trips a synthetic type over a struct array in both storage forms: a nullable array is stored wrapped as
     * {@code { repeated T values; }}, a non-nullable one as a plain repeated field, and the serializer picks the
     * constituent descriptor and nesting expression differently for each.
     */
    @ParameterizedTest(name = "nullableArray = {0}")
    @BooleanSource
    void testUnnestedSyntheticTypeSerializationAndDeserialization(final boolean nullableArray) {
        final var syntheticName = "__unnested_employees_score_idx";
        final var scoreType = struct("score",
                structField("label", DataType.Primitives.STRING.type(), 1),
                structField("value", DataType.Primitives.LONG.type(), 2));
        final var table = tableWithId("employees", "scores", DataType.ArrayType.from(scoreType, nullableArray));
        final var keyExpression = Key.Expressions.concat(
                constituentField("SQ", "label"),
                constituentField("row", "id"),
                constituentField("SQ", "value"));
        final var originalTemplate = templateWith(table,
                syntheticTable(syntheticName, table, "score_idx", keyExpression,
                        new RecordLayerUnnestedSyntheticTable.NestedConstituent("SQ", "row",
                                arrayElementsExpression("scores", nullableArray))),
                scoreType);

        // The nesting expression says how to reach the array elements, and it depends on the storage form: a
        // non-nullable array is a plain repeated field, a nullable one is wrapped in a holder message.
        final var recordMetaData = serializeWithSyntheticType(originalTemplate, syntheticName);
        Assertions.assertEquals(arrayElementsExpression("scores", nullableArray),
                serializedConstituent(recordMetaData, syntheticName).getNestingExpression());

        final var deserialized = deserializeSyntheticTable(recordMetaData);
        Assertions.assertEquals(syntheticName, deserialized.getName());
        Assertions.assertEquals("employees", deserialized.getParentTableName());
        Assertions.assertEquals("row", deserialized.getAlias());

        final var constituent = Iterables.getOnlyElement(deserialized.getConstituents());
        Assertions.assertEquals("SQ", constituent.getAlias());
        Assertions.assertEquals("row", constituent.getParentAlias());
        Assertions.assertEquals(arrayElementsExpression("scores", nullableArray), constituent.getNestingExpression());

        final var index = Iterables.getOnlyElement(deserialized.getIndexes());
        Assertions.assertEquals("score_idx", index.getName());
        Assertions.assertEquals(IndexTypes.VALUE, index.getIndexType());
        Assertions.assertEquals(keyExpression, KeyExpression.fromProto(index.getKeyExpression().toKeyExpression()));
    }

    /**
     * Synthetic tables are held in a {@code Set}, so they need value semantics: two structurally identical
     * instances must compare equal and collapse in a set, and a difference in any component must not.
     */
    @Test
    void unnestedSyntheticTableHasValueSemantics() {
        final var scoreType = struct("score",
                structField("label", DataType.Primitives.STRING.type(), 1),
                structField("value", DataType.Primitives.LONG.type(), 2));
        final var table = tableWithId("employees", "scores", DataType.ArrayType.from(scoreType, true));
        final var key = Key.Expressions.concat(constituentField("SQ", "label"), constituentField("row", "id"));
        final java.util.function.Supplier<RecordLayerUnnestedSyntheticTable> build = () ->
                syntheticTable("__unnested_employees_score_idx", table, "score_idx", key,
                        new RecordLayerUnnestedSyntheticTable.NestedConstituent("SQ", "row",
                                arrayElementsExpression("scores", true)));

        Assertions.assertEquals(build.get(), build.get());
        Assertions.assertEquals(build.get().hashCode(), build.get().hashCode());
        // Set.copyOf collapses duplicates, unlike Set.of which rejects them.
        Assertions.assertEquals(1, Set.copyOf(List.of(build.get(), build.get())).size());

        // A differing constituent must break equality — otherwise the set would silently collapse distinct types.
        final var differentConstituent = syntheticTable("__unnested_employees_score_idx", table, "score_idx", key,
                new RecordLayerUnnestedSyntheticTable.NestedConstituent("OTHER", "row",
                        arrayElementsExpression("scores", true)));
        Assertions.assertNotEquals(build.get(), differentConstituent);

        final var differentName = syntheticTable("__unnested_employees_other_idx", table, "score_idx", key,
                new RecordLayerUnnestedSyntheticTable.NestedConstituent("SQ", "row",
                        arrayElementsExpression("scores", true)));
        Assertions.assertNotEquals(build.get(), differentName);
    }

    /**
     * Round trips a synthetic type with two chained constituents, where the second unnests an array that lives on
     * the element type of the first.
     */
    @Test
    void testChainedUnnestedSyntheticTypeSerializationAndDeserialization() {
        final var syntheticName = "__unnested_nested_employees_chained_idx";
        final var innerType = struct("inner", structField("y", DataType.Primitives.STRING.type(), 1));
        final var outerType = struct("outer",
                structField("x", DataType.Primitives.STRING.type(), 1),
                structField("q", DataType.ArrayType.from(innerType, true), 2));
        final var table = tableWithId("nested_employees", "p", DataType.ArrayType.from(outerType, true));
        final var keyExpression = Key.Expressions.concat(
                constituentField("P_C", "x"),
                constituentField("row", "id"),
                constituentField("Q_C", "y"));
        final var originalTemplate = templateWith(table,
                syntheticTable(syntheticName, table, "chained_idx", keyExpression,
                        new RecordLayerUnnestedSyntheticTable.NestedConstituent("P_C", "row",
                                arrayElementsExpression("p", true)),
                        new RecordLayerUnnestedSyntheticTable.NestedConstituent("Q_C", "P_C",
                                arrayElementsExpression("q", true))),
                innerType, outerType);

        final var deserialized =
                deserializeSyntheticTable(serializeWithSyntheticType(originalTemplate, syntheticName));
        Assertions.assertEquals(syntheticName, deserialized.getName());
        Assertions.assertEquals("nested_employees", deserialized.getParentTableName());
        Assertions.assertEquals("row", deserialized.getAlias());

        // Both constituents must come back with the parent link and array field they went in with; the inner one
        // hangs off the outer constituent, not off the parent table.
        Assertions.assertEquals(List.of("P_C", "Q_C"), deserialized.getConstituents().stream()
                .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getAlias)
                .collect(Collectors.toList()));
        Assertions.assertEquals(List.of("row", "P_C"), deserialized.getConstituents().stream()
                .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getParentAlias)
                .collect(Collectors.toList()));
        Assertions.assertEquals(List.of(List.of("p", "values"), List.of("q", "values")),
                deserialized.getConstituents().stream()
                        .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getFieldPath)
                        .collect(Collectors.toList()));

        final var index = Iterables.getOnlyElement(deserialized.getIndexes());
        Assertions.assertEquals("chained_idx", index.getName());
        Assertions.assertEquals(keyExpression, KeyExpression.fromProto(index.getKeyExpression().toKeyExpression()));
    }

    /**
     * Round trips a constituent whose array is reached through two meaningful hops, {@code map.entry} — the shape
     * the record layer's own unnested record types use. Neither hop can be dropped and neither is the {@code values}
     * wrapper, so this is only representable because the nesting expression is stored rather than a field name.
     */
    @Test
    void testUnnestedSyntheticTypeOverTwoHopPathSerializationAndDeserialization() {
        final var syntheticName = "__unnested_map_records_map_idx";
        final var entryType = struct("entryType",
                structField("k", DataType.Primitives.STRING.type(), 1),
                structField("v", DataType.Primitives.LONG.type(), 2));
        // A non-nullable array, so `entry` is a plain repeated field rather than a `values` wrapper.
        final var mapHolderType = struct("mapHolder",
                structField("entry", DataType.ArrayType.from(entryType, false), 1));
        final var table = tableWithId("map_records", "map", mapHolderType);
        final var nestingExpression = Key.Expressions.field("map")
                .nest(Key.Expressions.field("entry", KeyExpression.FanType.FanOut));
        final var keyExpression = Key.Expressions.concat(
                constituentField("SQ", "k"),
                constituentField("row", "id"),
                constituentField("SQ", "v"));
        final var originalTemplate = templateWith(table,
                syntheticTable(syntheticName, table, "map_idx", keyExpression,
                        new RecordLayerUnnestedSyntheticTable.NestedConstituent("SQ", "row",
                                nestingExpression)),
                entryType, mapHolderType);

        final var recordMetaData = serializeWithSyntheticType(originalTemplate, syntheticName);
        final var nested = serializedConstituent(recordMetaData, syntheticName);
        Assertions.assertEquals(nestingExpression, nested.getNestingExpression());
        // The constituent has to be the element type, which is only reachable by walking both hops. Stopping at
        // `map` would yield the holder message, whose only field is `entry`.
        Assertions.assertEquals(List.of("k", "v"), nested.getRecordType().getDescriptor().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList()));

        final var constituent = Iterables.getOnlyElement(deserializeSyntheticTable(recordMetaData).getConstituents());
        Assertions.assertEquals(List.of("map", "entry"), constituent.getFieldPath());
        Assertions.assertEquals(nestingExpression, constituent.getNestingExpression());
    }

    @Test
    void testViewSerializationAndDeserialization() {
        // Create a schema template with a view
        final var originalTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("name")
                                .setDataType(DataType.Primitives.STRING.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("employee_view")
                        .setDescription("SELECT id, name FROM employees WHERE id > 100")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        // Verify the view is stored in the original template
        Assertions.assertEquals(1, originalTemplate.getViews().size());
        final var viewOpt = originalTemplate.findViewByName("employee_view");
        Assertions.assertTrue(viewOpt.isPresent());
        Assertions.assertEquals("employee_view", viewOpt.get().getName());
        Assertions.assertEquals("SELECT id, name FROM employees WHERE id > 100", viewOpt.get().getDescription());

        // Test serialization through RecordMetaData
        final var recordMetaData = originalTemplate.toRecordMetadata();
        final var deserializedTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(
                recordMetaData, "TestSchemaTemplate", 42);

        // Verify the view was preserved through serialization
        Assertions.assertEquals(1, deserializedTemplate.getViews().size());
        final var deserializedViewOpt = deserializedTemplate.findViewByName("employee_view");
        Assertions.assertTrue(deserializedViewOpt.isPresent());
        Assertions.assertEquals("employee_view", deserializedViewOpt.get().getName());
        Assertions.assertEquals("SELECT id, name FROM employees WHERE id > 100", deserializedViewOpt.get().getDescription());
    }

    @Test
    void testSchemaTemplateWithTablesAndViews() {
        // Create a complex schema with multiple tables and views
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("department")
                                .setDataType(DataType.Primitives.STRING.type())
                                .build())
                        .build())
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("departments")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("dept_id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("employee_view")
                        .setDescription("SELECT * FROM employees")
                        .setViewCompiler(ignored -> null)
                        .build())
                .addView(RecordLayerView.newBuilder()
                        .setName("department_view")
                        .setDescription("SELECT * FROM departments")
                        .setViewCompiler(ignored -> null)
                        .build())
                .build();

        // Verify both tables and views exist
        Assertions.assertEquals(2, schemaTemplate.getTables().size());
        Assertions.assertEquals(2, schemaTemplate.getViews().size());
        Assertions.assertTrue(schemaTemplate.findTableByName("employees").isPresent());
        Assertions.assertTrue(schemaTemplate.findTableByName("departments").isPresent());
        Assertions.assertTrue(schemaTemplate.findViewByName("employee_view").isPresent());
        Assertions.assertTrue(schemaTemplate.findViewByName("department_view").isPresent());
    }

    @Test
    void testViewBuilderToBuilder() {
        // Create a view and convert to builder and back
        final var originalView = RecordLayerView.newBuilder()
                .setName("test_view")
                .setDescription("SELECT * FROM employees")
                .setViewCompiler(ignored -> null)
                .build();

        // Convert to builder and back
        final var rebuiltView = originalView.toBuilder().build();

        // Verify all properties are preserved
        Assertions.assertEquals(originalView.getName(), rebuiltView.getName());
        Assertions.assertEquals(originalView.getDescription(), rebuiltView.getDescription());
    }

    @Test
    void testFindViewByNameReturnsEmpty() {
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("employees")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .build();

        // Verify that finding a non-existent view returns empty
        final var viewOpt = schemaTemplate.findViewByName("non_existent_view");
        Assertions.assertFalse(viewOpt.isPresent());
    }

    @Nonnull
    private static Descriptors.FileDescriptor createEscapedRecordTypesDescriptor() {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_schema_with_escaping.proto")
                .setPackage("com.apple.foundationdb.record.test1")
                .setSyntax("proto2")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Foo__0Bar__1Baz__2End")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("id")
                                .setNumber(1)
                        )
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("a__0b__1c__2d")
                                .setNumber(2)
                        )
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("otherField")
                                .setNumber(3)
                        )
                )
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("RecordTypeUnion")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("Foo__0Bar__1Baz__2End")
                                .setName("_Foo__0Bar__1Baz__2End")
                                .setNumber(1)
                        )
                )
                .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            return Assertions.fail("unable to build file descriptor", e);
        }
    }

    @Nonnull
    private static Descriptors.FileDescriptor createRecordTypesDescriptorWithMalformedEscaping() {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_schema_with_malformed_escaping.proto")
                .setPackage("com.apple.foundationdb.record.test1")
                .setSyntax("proto2")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("_Foo__Bar__1Baz")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("id")
                                .setNumber(1)
                        )
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("a__b__1c__2d")
                                .setNumber(2)
                        )
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("otherField")
                                .setNumber(3)
                        )
                )
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("RecordTypeUnion")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("_Foo__Bar__1Baz")
                                .setName("__Foo__Bar__1Baz")
                                .setNumber(1)
                        )
                )
                .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            return Assertions.fail("unable to build file descriptor", e);
        }
    }

    private static final class RecordMetadataDeserializerWithPeekingFunctionSupplier extends RecordMetadataDeserializer {

        @Nonnull
        private final Map<String, Integer> invocationsCount;

        public RecordMetadataDeserializerWithPeekingFunctionSupplier(@Nonnull final RecordMetaData recordMetaData) {
            super(recordMetaData);
            invocationsCount = new HashMap<>();
            hookInvokedRoutines(builder, invocationsCount);
        }

        private static void hookInvokedRoutines(@Nonnull final RecordLayerSchemaTemplate.Builder schemaBuilder,
                                                @Nonnull final Map<String, Integer> invocationsCount) {
            final List<RecordLayerInvokedRoutine> invokedRoutines = schemaBuilder.getInvokedRoutines();
            for (RecordLayerInvokedRoutine routine : invokedRoutines) {
                final String name = routine.getName();
                final Function<Boolean, UserDefinedFunction> provider = routine.getUserDefinedFunctionProvider();
                final RecordLayerInvokedRoutine.Builder routineBuilder = routine.toBuilder();
                routineBuilder.withUserDefinedFunctionProvider(isCaseSensitive -> {
                    invocationsCount.merge(name, 1, Integer::sum);
                    return provider.apply(isCaseSensitive);
                });
                schemaBuilder.removeInvokedRoutine(name);
                schemaBuilder.addInvokedRoutine(routineBuilder.build());
            }
        }

        boolean hasNoCompilationRequestsFor(@Nonnull final String functionName) {
            return invocationsCount.get(functionName) == null;
        }

        boolean hasOneCompilationRequestFor(@Nonnull final String functionName) {
            return 1 == invocationsCount.get(functionName);
        }

        @Nonnull
        public PlanGenerator getPlanGenerator() throws RelationalException, SQLException {

            final var metricCollector = NoOpMetricCollector.INSTANCE;
            final PlanContext ctx = PlanContext.Builder.create()
                    .withConstantActionFactory(NoOpMetadataOperationsFactory.INSTANCE)
                    .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                    .withMetricsCollector(metricCollector)
                    .withDbUri(URI.create(""))
                    .withMetadata(getRecordMetaData())
                    .withSchemaTemplate(getSchemaTemplate("testSchema", 42))
                    .withPlannerConfiguration(PlannerConfiguration.ofAllAvailableIndexes())
                    .build();
            return PlanGenerator.create(Optional.empty(), ctx, ctx.getMetaData(), new RecordStoreState(null, Map.of()),
                   IndexMaintainerFactoryRegistryImpl.instance(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        }
    }


}
