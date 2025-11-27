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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataDeserializer;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
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

        // the RecordLayerSchemaTemplate deserializer translates proto fields to user-defined names.
        final var expectedTableName = ProtoUtils.toUserIdentifier("Foo__0Bar__1Baz__2End");

        final var expectedTable = RecordLayerTable.newBuilder(false)
                .setName(expectedTableName)
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName(ProtoUtils.toUserIdentifier("a__0b__1c__2d"))
                        .setDataType(DataType.Primitives.NULLABLE_LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("otherField")
                        .setDataType(DataType.Primitives.NULLABLE_STRING.type())
                        .build())
                .setPrimaryKey(primaryKey)
                .build();

        final var actualSchemaTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(metaDataBuilder.build(), "TestSchemaTemplate", 42);
        Assertions.assertEquals(1, actualSchemaTemplate.getTables().size());

        final var tableMaybe = actualSchemaTemplate.findTableByName(expectedTableName);
        Assertions.assertTrue(tableMaybe.isPresent());
        final var actualTable = tableMaybe.get();
        final var actualRecordTable = Assertions.assertInstanceOf(RecordLayerTable.class, actualTable);
        Assertions.assertEquals(expectedTable.getName(), actualRecordTable.getName());
        Assertions.assertEquals(expectedTable.getColumns(), actualRecordTable.getColumns());
        Assertions.assertEquals(expectedTable.getPrimaryKey(), actualRecordTable.getPrimaryKey());
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
                .withUserDefinedRoutine(ignored -> new CompiledFunctionStub())
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
                    .withUserDefinedRoutine(igored -> new CompiledFunctionStub())
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

    private static final class RecordMetadataDeserializerWithPeekingFunctionSupplier extends RecordMetadataDeserializer {

        @Nonnull
        private final Map<String, Integer> invocationsCount;

        public RecordMetadataDeserializerWithPeekingFunctionSupplier(@Nonnull final RecordMetaData recordMetaData) {
            super(recordMetaData);
            invocationsCount = new HashMap<>();
        }

        @Nonnull
        @Override
        protected Function<Boolean, com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction> getSqlFunctionCompiler(@Nonnull final String name,
                                                                                                                                  @Nonnull final Supplier<RecordLayerSchemaTemplate> metadata,
                                                                                                                                  @Nonnull final String functionBody) {
            return isCaseSensitive -> {
                invocationsCount.merge(name, 1, Integer::sum);
                return super.getSqlFunctionCompiler(name, metadata, functionBody).apply(isCaseSensitive);
            };
        }

        boolean hasNoCompilationRequestsFor(@Nonnull final String functionName) {
            return invocationsCount.get(functionName) == null;
        }

        boolean hasOneCompilationRequestFor(@Nonnull final String functionName) {
            return 1 == invocationsCount.get(functionName);
        }

        @Nonnull
        public PlanGenerator getPlanGenerator() throws RelationalException, SQLException {

            final var metricCollector = new MetricCollector() {
                @Override
                public void increment(@Nonnull RelationalMetric.RelationalCount count) {
                }

                @Override
                public <T> T clock(@Nonnull RelationalMetric.RelationalEvent event,
                                   com.apple.foundationdb.relational.util.Supplier<T> supplier) throws RelationalException {
                    return supplier.get();
                }
            };
            final PlanContext ctx = PlanContext.Builder.create()
                    .withConstantActionFactory(NoOpMetadataOperationsFactory.INSTANCE)
                    .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                    .withMetricsCollector(metricCollector)
                    .withDbUri(URI.create(""))
                    .withMetadata(getRecordMetaData())
                    .withSchemaTemplate(getSchemaTemplate("testSchema", 42))
                    .withPlannerConfiguration(PlannerConfiguration.ofAllAvailableIndexes())
                    .withUserVersion(0)
                    .build();
            return PlanGenerator.create(Optional.empty(), ctx, ctx.getMetaData(), new RecordStoreState(null, Map.of()),
                   IndexMaintainerFactoryRegistryImpl.instance(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        }
    }


}
