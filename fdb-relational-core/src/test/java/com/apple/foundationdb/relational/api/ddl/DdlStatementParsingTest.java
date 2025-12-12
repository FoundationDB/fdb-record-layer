/*
 * DdlStatementParsingTest.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedMacroFunction;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.DdlTestUtil.IndexedColumn;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metric.RecordLayerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.PermutationIterator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests that verify that the language behaves correctly and has nice features and stuff. It does _not_ verify
 * that the underlying execution is correct, only that the language is parsed as expected.
 */
public class DdlStatementParsingTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(DdlStatementParsingTest.class, TestSchemas.books(),
            Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build(), null);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA")
            .withOptions(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());

    public DdlStatementParsingTest() throws SQLException {
    }

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    private static final String[] validPrimitiveDataTypes = new String[] {
            "integer", "bigint", "double", "boolean", "string", "bytes", "vector(3, float)", "vector(4, double)", "vector(5, half)"
    };

    @Nonnull
    public static Stream<Arguments> columnTypePermutations() {
        int numColumns = 2;
        final List<String> items = List.of(validPrimitiveDataTypes);

        final PermutationIterator<String> permutations = PermutationIterator.generatePermutations(items, numColumns);
        return permutations.stream()
                .flatMap(permutation -> Arrays.stream(DdlTestUtil.IndexSyntax.values())
                        .map(syntax -> Arguments.of(syntax, permutation)));
    }

    void shouldFailWith(@Nonnull final String query, @Nullable final ErrorCode errorCode) throws Exception {
        connection.setAutoCommit(false);
        (connection.getUnderlyingEmbeddedConnection()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/DdlStatementParsingTest").getPlan(query));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldFailWithInjectedFactory(@Nonnull final String query, @Nullable final ErrorCode errorCode,
                                       @Nonnull final MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        connection.setAutoCommit(false);
        (connection.getUnderlyingEmbeddedConnection()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/DdlStatementParsingTest", metadataOperationsFactory).getPlan(query));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query,
                                       @Nonnull final MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        connection.setAutoCommit(false);
        (connection.getUnderlyingEmbeddedConnection()).createNewTransaction();
        final var transaction = connection.getUnderlyingEmbeddedConnection().getTransaction();
        final var plan = DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                "/DdlStatementParsingTest", metadataOperationsFactory, PreparedParams.empty(),
                Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build()).getPlan(query);
        // execute the plan so we run any extra test-driven verifications within the transactional closure.
        plan.execute(Plan.ExecutionContext.of(transaction, Options.NONE, connection,
                new RecordLayerMetricCollector(transaction.unwrap(RecordContextTransaction.class).getContext())));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldFailWithInjectedQueryFactory(@Nonnull final String query, @Nullable ErrorCode errorCode,
                                            @Nonnull final DdlQueryFactory queryFactory) throws Exception {
        connection.setAutoCommit(false);
        (connection.getUnderlyingEmbeddedConnection()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/DdlStatementParsingTest", queryFactory).getPlan(query));
        connection.rollback();
        connection.setAutoCommit(true);
        Assertions.assertEquals(errorCode, ve.getErrorCode());
    }

    void shouldWorkWithInjectedQueryFactory(@Nonnull final String query, @Nonnull DdlQueryFactory queryFactory) throws Exception {
        connection.setAutoCommit(false);
        (connection.getUnderlyingEmbeddedConnection()).createNewTransaction();
        final var transaction = connection.getUnderlyingEmbeddedConnection().getTransaction();
        final var plan = DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                "/DdlStatementParsingTest", queryFactory).getPlan(query);
        // execute the plan so we run any extra test-driven verifications within the transactional closure.
        plan.execute(Plan.ExecutionContext.of(transaction, Options.NONE, connection,
                new RecordLayerMetricCollector(transaction.unwrap(RecordContextTransaction.class).getContext())));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    @Nonnull
    private static DescriptorProtos.FileDescriptorProto getProtoDescriptor(@Nonnull final SchemaTemplate schemaTemplate) {
        Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, schemaTemplate);
        final var asRecordLayerSchemaTemplate = (RecordLayerSchemaTemplate)schemaTemplate;
        return asRecordLayerSchemaTemplate.toRecordMetadata().toProto().getRecords();
    }

    @EnumSource(DdlTestUtil.IndexSyntax.class)
    @ParameterizedTest
    void indexFailsWithNonExistingTable(DdlTestUtil.IndexSyntax indexSyntax) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "t_idx", List.of(new IndexedColumn("a")), List.of(), "foo");
        shouldFailWith(stmt, ErrorCode.INVALID_SCHEMA_TEMPLATE);
    }

    @EnumSource(DdlTestUtil.IndexSyntax.class)
    @ParameterizedTest
    void indexFailsWithNonExistingIndexColumn(DdlTestUtil.IndexSyntax indexSyntax) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE foo(a bigint, PRIMARY KEY(a)) " +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "t_idx", List.of(new IndexedColumn("non_existing")), List.of(), "foo");
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN);
    }

    @EnumSource(DdlTestUtil.IndexSyntax.class)
    @ParameterizedTest
    void indexFailsWithReservedKeywordAsName(DdlTestUtil.IndexSyntax indexSyntax) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "table", List.of(new IndexedColumn("a")), List.of(), "foo");
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void enumFailsWithNoOptions() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS ENUM foo () " +
                "CREATE TABLE bar (id bigint, foo_field foo, PRIMARY KEY(id))";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void enumFailsWithUnquotedOptions() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS ENUM foo (OPTION_1, OPTION_2) " +
                "CREATE TABLE bar (id bigint, foo_field foo, PRIMARY KEY(id))";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void basicEnumParsedCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS ENUM my_enum ('VAL_1', 'VAL_2') " +
                "CREATE TABLE my_table (id bigint, enum_field my_enum, PRIMARY KEY(id))";

        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate)template).getTables().size(), "should have only 1 table");
                DescriptorProtos.FileDescriptorProto fileDescriptorProto = getProtoDescriptor(template);
                Assertions.assertEquals(1, fileDescriptorProto.getEnumTypeCount(), "should have one enum defined");
                fileDescriptorProto.getEnumTypeList().forEach(enumDescriptorProto -> {
                    Assertions.assertEquals("my_enum", enumDescriptorProto.getName());
                    Assertions.assertEquals(2, enumDescriptorProto.getValueCount());
                    Assertions.assertEquals(List.of("VAL_1", "VAL_2"), enumDescriptorProto.getValueList().stream()
                            .map(DescriptorProtos.EnumValueDescriptorProto::getName)
                            .collect(Collectors.toList()));
                });

                return txn -> {
                };
            }
        });
    }

    private static Stream<Arguments> typesMap() {
        return Stream.of(
                Arguments.of(Types.INTEGER, "INTEGER"),
                Arguments.of(Types.BIGINT, "BIGINT"),
                Arguments.of(Types.FLOAT, "FLOAT"),
                Arguments.of(Types.DOUBLE, "DOUBLE"),
                Arguments.of(Types.VARCHAR, "STRING"),
                Arguments.of(Types.BOOLEAN, "BOOLEAN"),
                Arguments.of(Types.BINARY, "BYTES"),
                Arguments.of(Types.STRUCT, "baz"),
                Arguments.of(Types.ARRAY, "STRING ARRAY")
        );
    }

    @ParameterizedTest
    @MethodSource("typesMap")
    void columnTypeWithNull(int sqlType, @Nonnull String sqlTypeName) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TABLE bar (id bigint, foo_field " + sqlTypeName + " null, PRIMARY KEY(id))";
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template, @Nonnull final Options templateProperties) {
                checkColumnNullability(template, sqlType, true);
                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("typesMap")
    void columnTypeWithNotNull(int sqlType, @Nonnull String sqlTypeName) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TABLE bar (id bigint, foo_field " + sqlTypeName + " not null, PRIMARY KEY(id))";
        if (sqlType == Types.ARRAY) {
            shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
                @Nonnull
                @Override
                public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template, @Nonnull final Options templateProperties) {
                    checkColumnNullability(template, sqlType, false);
                    return txn -> {
                    };
                }
            });
        } else {
            shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION);
        }
    }


    @Test
    void failsToParseEmptyTemplateStatements() throws Exception {
        //empty template statements are invalid, and can be rejected in the parser
        final String stmt = "CREATE SCHEMA TEMPLATE test_template ";
        boolean[] visited = new boolean[] {false};
        shouldFailWithInjectedFactory(stmt, ErrorCode.SYNTAX_ERROR, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                Assertions.assertEquals(0, ((RecordLayerSchemaTemplate)template).getTables().size(), "Tables defined!");
                visited[0] = true;
                return txn -> {
                };
            }
        });
        Assertions.assertFalse(visited[0], "called for a constant action!");
    }

    @Test
    void createTypeWithPrimaryKeyFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT t (a bigint, b string, PRIMARY KEY(b))";
        shouldFailWithInjectedFactory(stmt, ErrorCode.SYNTAX_ERROR, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.fail("Should fail during parsing!");
                return txn -> {
                };
            }
        });
    }

    /**
     * Validate that Protobuf escaping on a schema template by looking at the produced meta-data.
     * This works with the tests in {@code valid-identifiers.yamsql}, which validate actual query semantics
     * on such a meta-data. This test allows us to validate which parts of the meta-data are actually
     * translated (it should only be things that get turned into Protobuf identifiers, like message types,
     * field names, and enum values) and which parts are preserved (like function, view, and index names).
     *
     * @throws Exception from generating the schema-template
     */
    @Test
    void translateNames() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT \"a.b$c__struct\" (\"___a$\" bigint, \"_b.x\" string, \"c__\" bigint)  " +
                "CREATE TYPE AS ENUM \"a.b$c__enum\" ('___A$', '_B.X', 'C__')  " +
                "CREATE TABLE \"__4a.b$c__table\"(\"__h__s\" \"a.b$c__struct\", \"_x.y\" bigint, \"enum.field\" \"a.b$c__enum\", primary key (\"__h__s\".\"_b.x\")) " +
                "CREATE INDEX \"a.b$c__index\" AS SELECT \"_x.y\", \"__h__s\".\"___a$\", \"__h__s\".\"c__\" FROM \"__4a.b$c__table\" ORDER BY \"_x.y\", \"__h__s\".\"___a$\" " +
                "CREATE VIEW \"a.b$c__view\" AS SELECT \"__h__s\".\"___a$\" AS \"f__00\" FROM \"__4a.b$c__table\" WHERE \"_x.y\" > 4 " +
                "CREATE FUNCTION \"a.b$c__function\"(in \"__param__int\" bigint, in \"__param__enum\" TYPE \"a.b$c__enum\") " +
                "  AS SELECT \"__h__s\".\"___a$\" AS \"f__00\" FROM \"__4a.b$c__table\" WHERE \"_x.y\" > \"__param__int\" AND \"enum.field\" = \"__param__enum\" " +
                "CREATE FUNCTION \"a.b$c__macro_function\"(in \"__in__4a.b$c__table\" TYPE \"__4a.b$c__table\") RETURNS string AS \"__in__4a.b$c__table\".\"__h__s\".\"_b.x\" ";

        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template, @Nonnull final Options templateProperties) {
                try {
                    // Assert all the user-visible names look like the user identifiers in the schema

                    Set<? extends Table> tables = template.getTables();
                    Assertions.assertEquals(1, tables.size(), () -> "tables " + tables + " should have only one element");

                    final Table table = Iterables.getOnlyElement(tables);
                    Assertions.assertEquals("__4a.b$c__table", table.getName());

                    final DataType.StructType structType = DataType.StructType.from("a.b$c__struct", List.of(
                            DataType.StructType.Field.from("___a$", DataType.LongType.nullable(), 1),
                            DataType.StructType.Field.from("_b.x", DataType.StringType.nullable(), 2),
                            DataType.StructType.Field.from("c__", DataType.LongType.nullable(), 3)
                    ), true);
                    final DataType.EnumType enumType = DataType.EnumType.from("a.b$c__enum", List.of(
                            DataType.EnumType.EnumValue.of("___A$", 0),
                            DataType.EnumType.EnumValue.of("_B.X", 1),
                            DataType.EnumType.EnumValue.of("C__", 2)
                    ), true);
                    Assertions.assertEquals(List.of(
                            RecordLayerColumn.newBuilder().setName("__h__s").setDataType(structType).setIndex(1).build(),
                            RecordLayerColumn.newBuilder().setName("_x.y").setDataType(DataType.LongType.nullable()).setIndex(2).build(),
                            RecordLayerColumn.newBuilder().setName("enum.field").setDataType(enumType).setIndex(3).build()
                    ), table.getColumns());

                    Set<? extends Index> indexes = table.getIndexes();
                    Assertions.assertEquals(1, indexes.size(), () -> "indexes " + indexes + " for table " + table.getName() + " should have only one element");
                    Index index = Iterables.getOnlyElement(indexes);
                    Assertions.assertEquals("a.b$c__index", index.getName());
                    Assertions.assertEquals("__4a.b$c__table", index.getTableName());

                    Set<? extends View> views = template.getViews();
                    Assertions.assertEquals(1, views.size(), () -> "views " + views + " should have only one element");
                    View view = Iterables.getOnlyElement(views);
                    Assertions.assertEquals("a.b$c__view", view.getName());

                    template.findInvokedRoutineByName("a.b$c__function")
                            .orElseGet(() -> Assertions.fail("could not find function a.b$c__function"));
                    template.findInvokedRoutineByName("a.b$c__macro_function")
                            .orElseGet(() -> Assertions.fail("could not find function a.b$c__macro_function"));

                    // Assert all the internal fields are using escaped protobuf identifiers

                    Assertions.assertInstanceOf(RecordLayerTable.class, table);
                    final RecordLayerTable recordLayerTable = (RecordLayerTable) table;
                    Assertions.assertEquals(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("__h__0s").nest("_b__2x")), recordLayerTable.getPrimaryKey());

                    Assertions.assertInstanceOf(RecordLayerIndex.class, index);
                    final RecordLayerIndex recordLayerIndex = (RecordLayerIndex) index;
                    Assertions.assertEquals(Key.Expressions.keyWithValue(Key.Expressions.concat(
                                    Key.Expressions.field("_x__2y"),
                                    Key.Expressions.field("__h__0s").nest(
                                            Key.Expressions.concatenateFields("___a__1", "c__0")
                                    )
                            ), 2),
                            recordLayerIndex.getKeyExpression());

                    Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                    final RecordLayerSchemaTemplate recordLayerSchemaTemplate = (RecordLayerSchemaTemplate) template;
                    final RecordMetaData metaData = recordLayerSchemaTemplate.toRecordMetadata();

                    Assertions.assertFalse(metaData.getRecordTypes().containsKey("__4a.b$c__table"), () -> "meta-data should not contain unescaped table name " + table.getName());
                    Assertions.assertTrue(metaData.getRecordTypes().containsKey("__4a__2b__1c__0table"), () -> "meta-data should contain unescaped table name of " + table.getName());
                    final RecordType recordType = metaData.getRecordType("__4a__2b__1c__0table");
                    final Descriptors.Descriptor typeDescriptor = recordType.getDescriptor();
                    Assertions.assertEquals("__4a__2b__1c__0table", typeDescriptor.getName());
                    Assertions.assertEquals(List.of("__h__0s", "_x__2y", "enum__2field"), typeDescriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList()));
                    final Descriptors.Descriptor structDescriptor = typeDescriptor.findFieldByName("__h__0s").getMessageType();
                    Assertions.assertEquals("a__2b__1c__0struct", structDescriptor.getName());
                    Assertions.assertEquals(List.of("___a__1", "_b__2x", "c__0"), structDescriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList()));
                    final Descriptors.EnumDescriptor enumDescriptor = typeDescriptor.findFieldByName("enum__2field").getEnumType();
                    Assertions.assertEquals("a__2b__1c__0enum", enumDescriptor.getName());
                    Assertions.assertEquals(List.of("___A__1", "_B__2X", "C__0"), enumDescriptor.getValues().stream().map(Descriptors.EnumValueDescriptor::getName).collect(Collectors.toList()));

                    var metaDataIndex = metaData.getIndex(index.getName());
                    Assertions.assertEquals("a.b$c__index", metaDataIndex.getName()); // Index name is _not_ translated
                    Assertions.assertEquals(recordLayerIndex.getKeyExpression(), metaDataIndex.getRootExpression()); // key expression is already validated as translated

                    final Map<String, com.apple.foundationdb.record.metadata.View> viewMap = metaData.getViewMap();
                    Assertions.assertTrue(viewMap.containsKey("a.b$c__view"), "should contain function a.b$c__view without escaping name");

                    final Map<String, UserDefinedFunction> functionMap = metaData.getUserDefinedFunctionMap();
                    Assertions.assertTrue(functionMap.containsKey("a.b$c__function"), "should contain function a.b$c__function without escaping name");
                    final UserDefinedFunction sqlFunction = functionMap.get("a.b$c__function");
                    Assertions.assertInstanceOf(RawSqlFunction.class, sqlFunction);
                    Assertions.assertTrue(functionMap.containsKey("a.b$c__macro_function"), "should contain function a.b$c__macro_function without escaping name");
                    final UserDefinedFunction macroFunction = functionMap.get("a.b$c__macro_function");
                    Assertions.assertInstanceOf(UserDefinedMacroFunction.class, macroFunction);

                    // Validates that referenced fields and types all line up
                    final MetaDataValidator validator = new MetaDataValidator(metaData, IndexMaintainerFactoryRegistryImpl.instance());
                    Assertions.assertDoesNotThrow(validator::validate, "Meta-data validation should complete successfully");
                } catch (RelationalException e) {
                    return Assertions.fail(e);
                }

                return txn -> {
                };
            }
        });
    }

    @Nonnull
    private static Stream<Arguments> invalidVectorTypes() {
        return Stream.of(
                // Zero dimensions
                Arguments.of("vector(0, float)"),
                // Negative dimensions
                Arguments.of("vector(-1, float)"),
                // Invalid element type
                Arguments.of("vector(3, integer)"),
                Arguments.of("vector(3, bigint)"),
                Arguments.of("vector(3, string)"),
                Arguments.of("vector(3, boolean)"),
                Arguments.of("vector(3, bytes)"),
                Arguments.of("vector(3, int)"),
                // Missing dimensions
                Arguments.of("vector(float)"),
                // Missing element type
                Arguments.of("vector(3)"),
                // Empty vector
                Arguments.of("vector()"),
                // Wrong order (type, dimensions)
                Arguments.of("vector(float, 3)"),
                // Non-numeric dimensions
                Arguments.of("vector(abc, float)"),
                // Decimal dimensions
                Arguments.of("vector(3.5, float)"),
                // Multiple commas
                Arguments.of("vector(3,, float)"),
                // Extra parameters
                Arguments.of("vector(3, float, extra)"),
                // Case variations of invalid types
                Arguments.of("vector(3, FLOAT32)"),
                Arguments.of("vector(3, DOUBLE64)")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidVectorTypes")
    void createInvalidVectorType(String vectorType) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE test_table (id bigint, vec_col " + vectorType + ", PRIMARY KEY(id))";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR);
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithOutOfOrderDefinitionsWork(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(indexSyntax == DdlTestUtil.IndexSyntax.INDEX_AS_SYNTAX);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) +
                "CREATE TYPE AS STRUCT FOO " + makeColumnDefinition(columns, false);

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate)template).getTables().size(), "Incorrect number of tables");
                return txn -> {
                };
            }
        });
    }

    /*Schema Template tests*/
    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplates(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(indexSyntax == DdlTestUtil.IndexSyntax.INDEX_AS_SYNTAX);
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TYPE AS STRUCT foo " + makeColumnDefinition(columns, false) +
                " CREATE TABLE bar (col0 bigint, col1 foo, PRIMARY KEY(col0))";
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getName(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(getProtoDescriptor(template));
                Assertions.assertEquals(1, schema.getTables().size(), "Incorrect number of tables");
                return txn -> {
                    try {
                        final DdlTestUtil.ParsedType type = schema.getType("foo");
                        assertColumnsMatch(type, columns);
                    } catch (Exception ve) {
                        throw ExceptionUtil.toRelationalException(ve);
                    }
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateTableWithOnlyRecordType(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(indexSyntax == DdlTestUtil.IndexSyntax.INDEX_AS_SYNTAX);
        final String baseTableDef = replaceLast(makeColumnDefinition(columns, false), ')', ", SINGLE ROW ONLY)");
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template  " +
                "CREATE TABLE foo " + baseTableDef;

        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getName(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(getProtoDescriptor(template));
                Assertions.assertEquals(1, schema.getTables().size(), "Incorrect number of tables");
                return txn -> {
                    try {
                        final DdlTestUtil.ParsedType type = schema.getTable("foo");
                        assertColumnsMatch(type, columns);
                    } catch (Exception ve) {
                        throw ExceptionUtil.toRelationalException(ve);
                    }
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithDuplicateIndexesFails(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        final String baseTableDef = makeColumnDefinition(columns, true);
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE FOO " + baseTableDef +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "foo_idx", List.of(new IndexedColumn("col0")), List.of(), "foo") +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "foo_idx", List.of(new IndexedColumn("col1")), List.of(), "foo"); //duplicate with the same name  on same table should fail

        shouldFailWithInjectedFactory(columnStatement, ErrorCode.INDEX_ALREADY_EXISTS, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.fail("Should not call this!");
                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithIndex(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        final List<String> indexColumns = chooseIndexColumns(columns, n -> n % 2 == 0);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template  " +
                "CREATE TYPE AS STRUCT foo " + makeColumnDefinition(columns, false) +
                "CREATE TABLE tbl " + makeColumnDefinition(columns, true) +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "v_idx", indexColumns.stream().map(IndexedColumn::new).collect(Collectors.toList()), List.of(), "tbl");

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate)template).getTables().size(), "Incorrect number of tables");
                Table info = ((RecordLayerSchemaTemplate)template).getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final Index index = Assert.optionalUnchecked(info.getIndexes().stream().findFirst());
                Assertions.assertEquals("v_idx", index.getName(), "Incorrect index name!");

                final var actualKe = ((RecordLayerIndex)index).getKeyExpression().toKeyExpression();
                List<RecordKeyExpressionProto.KeyExpression> keys = null;
                if (actualKe.hasThen()) {
                    keys = new ArrayList<>(actualKe.getThen().getChildList());
                } else if (actualKe.hasField()) {
                    keys = new ArrayList<>();
                    keys.add(actualKe);
                } else {
                    Assertions.fail("Unexpected KeyExpression type");
                }
                //if the first key is RecordType,remove that
                if (keys.get(0).hasRecordTypeKey()) {
                    keys.remove(0);
                }

                List<String> idxColumns = chooseIndexColumns(columns, n -> n % 2 == 0);
                for (int i = 0; i < idxColumns.size(); i++) {
                    Assertions.assertEquals(idxColumns.get(i), keys.get(i).getField().getFieldName(), "Incorrect column at position " + i);
                }
                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithIndexAndInclude(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(columns.size() > 1); //the test only works with multiple columns
        final List<String> indexedColumns = chooseIndexColumns(columns, n -> n % 2 == 0); //choose every other column
        final List<String> unindexedColumns = chooseIndexColumns(columns, n -> n % 2 != 0);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TYPE AS STRUCT foo " + makeColumnDefinition(columns, false) +
                " CREATE TABLE tbl " + makeColumnDefinition(columns, true) +
                DdlTestUtil.generateIndexDdlStatement(indexSyntax, "v_idx", indexedColumns.stream().map(IndexedColumn::new).collect(Collectors.toList()), unindexedColumns, "tbl");
        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate)template).getTables().size(), "Incorrect number of tables");
                Table info = ((RecordLayerSchemaTemplate)template).getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final Index index = Assert.optionalUnchecked(info.getIndexes().stream().findFirst());
                Assertions.assertEquals("v_idx", index.getName(), "Incorrect index name!");

                RecordKeyExpressionProto.KeyExpression actualKe = ((RecordLayerIndex)index).getKeyExpression().toKeyExpression();
                Assertions.assertNotNull(actualKe.getKeyWithValue(), "Null KeyExpression for included columns!");
                final RecordKeyExpressionProto.KeyWithValue keyWithValue = actualKe.getKeyWithValue();

                //This is a weird workaround for the problem fixed in https://github.com/FoundationDB/fdb-record-layer/pull/1585,
                // once that's been merged and we get a release that contains it, we can replace this with a more
                //natural api
                final RecordKeyExpressionProto.KeyExpression innerKey = keyWithValue.getInnerKey();
                int splitPoint = keyWithValue.getSplitPoint();
                final ThenKeyExpression then = new ThenKeyExpression(innerKey.getThen());
                KeyExpression keyExpr = then.getSubKey(0, splitPoint);
                KeyExpression valueExpr = then.getSubKey(splitPoint, then.getColumnSize());

                Assertions.assertEquals(indexedColumns.size(), keyExpr.getColumnSize(), "Incorrect number of parsed columns!");
                for (int i = 0; i < indexedColumns.size(); i++) {
                    final RecordKeyExpressionProto.KeyExpression ke = keyExpr.getSubKey(i, i + 1).toKeyExpression();
                    Assertions.assertEquals(indexedColumns.get(i), ke.getField().getFieldName(), "Incorrect column at position " + i);
                }

                Assertions.assertEquals(unindexedColumns.size(), valueExpr.getColumnSize(), "Incorrect number of parsed columns!");
                for (int i = 0; i < unindexedColumns.size(); i++) {
                    final RecordKeyExpressionProto.KeyExpression ve = valueExpr.getSubKey(i, i + 1).toKeyExpression();
                    Assertions.assertEquals(unindexedColumns.get(i), ve.getField().getFieldName(), "Incorrect column at position " + i);
                }

                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(booleans = {true, false})
    void createSchemaTemplateSplitLongRecord(Boolean enableLongRows) throws Exception {
        String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TABLE test_table (A BIGINT, PRIMARY KEY(A))";
        if (enableLongRows != null) {
            templateStatement += " WITH OPTIONS (ENABLE_LONG_ROWS = " + enableLongRows + ")";
        }
        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                if (enableLongRows == null || enableLongRows) {
                    Assertions.assertTrue(template.isEnableLongRows());
                } else {
                    Assertions.assertFalse(template.isEnableLongRows());
                }
                return txn -> {
                };
            }
        });
    }

    @Test
    void dropSchemaTemplates() throws Exception {
        final String columnStatement = "DROP SCHEMA TEMPLATE test_template";
        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
                Assertions.assertEquals("test_template", templateId, "Incorrect schema template name!");
                called[0] = true;
                return txn -> {
                };
            }
        });
        Assertions.assertTrue(called[0], "Did not call CA method!");
    }

    @Test
    void createSchemaTemplateWithNoTypesFails() throws Exception {
        final String command = "CREATE SCHEMA TEMPLATE no_types ;"; // parser rules design doesn't permit this case.

        shouldFailWithInjectedFactory(command, ErrorCode.SYNTAX_ERROR, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                Assertions.fail("Should fail with a parser error");
                return super.getSaveSchemaTemplateConstantAction(template, templateProperties);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createTable(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(indexSyntax == DdlTestUtil.IndexSyntax.INDEX_AS_SYNTAX);
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template CREATE TABLE foo " +
                makeColumnDefinition(columns, true);
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getName(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(getProtoDescriptor(template));
                Assertions.assertEquals(1, schema.getTables().size(), "Incorrect number of tables");
                return txn -> {
                    try {
                        final DdlTestUtil.ParsedType table = schema.getTable("foo");
                        assertColumnsMatch(table, columns);
                    } catch (Exception ve) {
                        throw ExceptionUtil.toRelationalException(ve);
                    }
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createTableAndType(DdlTestUtil.IndexSyntax indexSyntax, List<String> columns) throws Exception {
        Assumptions.assumeTrue(indexSyntax == DdlTestUtil.IndexSyntax.INDEX_AS_SYNTAX);
        final String typeDef = "CREATE TYPE AS STRUCT typ " + makeColumnDefinition(columns, false);
        // current implementation of metadata prunes unused types in the serialization, this may or may not
        // be something we want to commit to long term.
        final var columnsWithType = ImmutableList.<String>builder().addAll(columns).add("typ").build();
        final String tableDef = "CREATE TABLE tbl " + makeColumnDefinition(columnsWithType, true);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                typeDef + " " +
                tableDef;

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getName(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(getProtoDescriptor(template));
                Assertions.assertEquals(1, schema.getTables().size(), "Incorrect number of tables");
                return txn -> {
                    try {
                        assertColumnsMatch(schema.getTable("tbl"), columnsWithType);
                        assertColumnsMatch(schema.getType("typ"), columns);
                    } catch (Exception ve) {
                        throw ExceptionUtil.toRelationalException(ve);
                    }
                };
            }
        });
    }

    /*Database tests*/
    @Test
    void createDatabase() throws Exception {
        final String command = "CREATE DATABASE /db_path";

        shouldWorkWithInjectedFactory(command, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
                Assertions.assertEquals(URI.create("/db_path"), dbPath, "Incorrect database path!");
                return NoOpMetadataOperationsFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath, constantActionOptions);
            }
        });
    }

    @Test
    void createDatabaseWithInvalidPathFails() throws Exception {
        final String command = "CREATE DATABASE not_a_path";

        shouldFailWithInjectedFactory(command, ErrorCode.INVALID_PATH, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
                Assertions.fail("We should not reach this point! We should throw a RelationalException instead");
                return NoOpMetadataOperationsFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath, constantActionOptions);
            }
        });
    }

    @Test
    void dropDatabase() throws Exception {
        final String command = "DROP DATABASE \"/db_path\"";

        shouldWorkWithInjectedFactory(command, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
                Assertions.assertEquals(URI.create("/db_path"), dbUrl, "Incorrect database path!");
                return NoOpMetadataOperationsFactory.INSTANCE.getDropDatabaseConstantAction(dbUrl, throwIfDoesNotExist, options);
            }
        });
    }

    @Test
    void dropDatabaseWithInvalidPathFails() throws Exception {
        final String command = "DROP DATABASE not_a_path";

        shouldFailWithInjectedFactory(command, ErrorCode.INVALID_PATH, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, boolean throwIfDoesNotExist, @Nonnull Options options) {
                Assertions.fail("We should not reach this point! We should throw a RelationalException instead");
                return NoOpMetadataOperationsFactory.INSTANCE.getCreateDatabaseConstantAction(dbUrl, options);
            }
        });
    }

    @Test
    void listDatabasesWithoutPrefixParsesCorrectly() throws Exception {
        final String command = "SHOW DATABASES";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory(command, new AbstractQueryFactory() {
            @Override
            public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
                called[0] = true;
                Assertions.assertNotNull(prefixPath, "Null URI passed!");
                Assertions.assertEquals(URI.create("/" + DdlStatementParsingTest.class.getSimpleName()), prefixPath, "incorrect root path specified!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }

        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void listDatabasesWithPrefixParsesCorrectly() throws Exception {
        final String command = "SHOW DATABASES WITH PREFIX /prefix";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory(command, new AbstractQueryFactory() {

            @Override
            public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
                called[0] = true;
                Assertions.assertNotNull(prefixPath, "Null URI passed!");
                Assertions.assertEquals(URI.create("/PREFIX"), prefixPath, "incorrect prefixed path specified!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }

        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void listSchemaTemplatesParsesProperly() throws Exception {
        final String command = "SHOW SCHEMA TEMPLATES";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory(command, new AbstractQueryFactory() {
            @Override
            public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
                Assertions.fail("Incorrectly called listSchemas!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }

            @Override
            public DdlQuery getListSchemaTemplatesQueryAction() {
                called[0] = true;
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void listSchemaTemplatesMissingSchemaFails() throws Exception {
        final String command = "SHOW TEMPLATES";

        shouldFailWithInjectedQueryFactory(command, ErrorCode.SYNTAX_ERROR, new AbstractQueryFactory() {
            @Override
            public DdlQuery getListSchemaTemplatesQueryAction() {
                Assertions.fail("Should not have called this method");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void describeSchemaTemplate() throws Exception {
        final String templateName = "TEST_TEMPLATE";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory("DESCRIBE SCHEMA TEMPLATE " + templateName, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                called[0] = true;
                Assertions.assertNotNull(schemaId, "Passed a null schema id!");
                Assertions.assertEquals(templateName, schemaId, "Incorrect template name!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void describeSchemaTemplateFailsWithNoTemplateId() throws Exception {
        final String query = "DESCRIBE SCHEMA TEMPLATE"; // parser rules design doesn't permit this case.

        shouldFailWithInjectedQueryFactory(query, ErrorCode.SYNTAX_ERROR, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void describeFailsWithNoIdentifier() throws Exception {
        final String query = "DESCRIBE "; // parser rules design doesn't permit this case.

        shouldFailWithInjectedQueryFactory(query, ErrorCode.SYNTAX_ERROR, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void describeSchemaSucceedsWithoutDatabase() throws Exception { // because parser falls back to connection's database.
        final String templateName = "TEST_TEMPLATE";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory("DESCRIBE SCHEMA " + templateName, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbUri, @Nonnull String schemaId) {
                called[0] = true;
                Assertions.assertNotNull(schemaId, "Passed a null schema id!");
                Assertions.assertNotNull(dbUri, "Passed a null db id!");
                Assertions.assertEquals(templateName, schemaId, "Incorrect template name!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void describeSchemaPathSucceeds() throws Exception {
        final String templateName = "TEST_TEMPLATE";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory("DESCRIBE SCHEMA " + "/test_db/" + templateName, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbUri, @Nonnull String schemaId) {
                called[0] = true;
                Assertions.assertNotNull(schemaId, "Passed a null schema id!");
                Assertions.assertNotNull(dbUri, "Passed a null db id!");
                Assertions.assertEquals(templateName, schemaId, "Incorrect template name!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void describeSchemaWithSetDatabaseSucceeds() throws Exception {
        final String templateName = "TEST_TEMPLATE";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedQueryFactory("DESCRIBE SCHEMA " + templateName, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbUri, @Nonnull String schemaId) {
                called[0] = true;
                Assertions.assertNotNull(schemaId, "Passed a null schema id!");
                Assertions.assertNotNull(dbUri, "Passed a null db id!");
                Assertions.assertEquals(templateName, schemaId, "Incorrect template name!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    void createSchemaWithPath() throws Exception {
        final String templateName = "test_template";

        boolean[] called = new boolean[] {false};
        shouldWorkWithInjectedFactory("CREATE SCHEMA /test_db/" + templateName + " WITH TEMPLATE " + templateName, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri,
                                                                @Nonnull String schemaName,
                                                                @Nonnull String templateId,
                                                                Options constantActionOptions) {
                called[0] = true;
                Assertions.assertNotNull(dbUri, "No database URI specified");
                Assertions.assertNotNull(schemaName, "No schema specified");
                Assertions.assertNotNull(templateId, "No template specified");
                return txn -> {
                };
            }
        });
        Assertions.assertTrue(called[0], "Did not call the correct method!");
    }

    @Test
    public void bitmapIndexCreationShouldWork() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE blahblah " +
                "CREATE TABLE msgstate(id string, uid bigint, mboxRef string, isSeen bigint, PRIMARY KEY(id)) " +
                "CREATE INDEX all_seen_uids_bitmap AS SELECT bitmap_construct_agg(bitmap_bit_position(uid)) FROM msgstate GROUP BY mboxRef, isSeen, bitmap_bucket_offset(uid)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                return txn -> {
                };
            }
        });
    }

    @Test
    void createViewWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW v AS SELECT * FROM bar";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var viewMaybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v"));
                assertThat(viewMaybe).isPresent();
                assertThat(Assert.optionalUnchecked(viewMaybe).getDescription()).isEqualTo("SELECT * FROM bar");
                return txn -> {
                };
            }
        });
    }

    @Test
    void createNestedViewWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW v1 AS SELECT * FROM bar " +
                "CREATE VIEW v2 AS SELECT * FROM v1";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var view1Maybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v1"));
                assertThat(view1Maybe).isPresent();
                assertThat(Assert.optionalUnchecked(view1Maybe).getDescription()).isEqualTo("SELECT * FROM bar");
                final var view2Maybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v2"));
                assertThat(view2Maybe).isPresent();
                assertThat(Assert.optionalUnchecked(view2Maybe).getDescription()).isEqualTo("SELECT * FROM v1");
                return txn -> {
                };
            }
        });
    }

    @Test
    void createViewWithJoinWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW v1 AS SELECT * FROM bar, bar " +
                "CREATE VIEW v2 AS SELECT * FROM v1, v1";

        shouldWorkWithInjectedFactory(schemaStatement, NoOpMetadataOperationsFactory.INSTANCE);
    }

    @Test
    void createViewWithCollidingNameDoesNotWorkCase1() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW bar AS SELECT * FROM bar";

        shouldFailWithInjectedQueryFactory(schemaStatement, ErrorCode.INVALID_SCHEMA_TEMPLATE, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void createViewWithCollidingNameDoesNotWorkCase2() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW v AS SELECT * FROM bar " +
                "CREATE TABLE v(id bigint, primary key(id))";

        shouldFailWithInjectedQueryFactory(schemaStatement, ErrorCode.INVALID_SCHEMA_TEMPLATE, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void createInvalidViewDefinitionDoesNotWork() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE VIEW v AS SELECTBLA * FROM bar ";

        shouldFailWithInjectedQueryFactory(schemaStatement, ErrorCode.SYNTAX_ERROR, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void createViewWithReferencesToSubsequentlyDefinedTableWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE VIEW v AS SELECT * FROM bar " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) ";

        shouldWorkWithInjectedFactory(schemaStatement, NoOpMetadataOperationsFactory.INSTANCE);
    }

    @Test
    void createViewWithCteWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE VIEW v AS WITH C1 AS (SELECT foo_field, id, baz_field FROM bar where id > 20) SELECT * FROM C1 " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) ";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var viewMaybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v"));
                assertThat(viewMaybe).isPresent();
                assertThat(Assert.optionalUnchecked(viewMaybe).getDescription()).isEqualTo("WITH C1 AS (SELECT foo_field, id, baz_field FROM bar where id > 20) SELECT * FROM C1");
                return txn -> {
                };
            }
        });
    }

    @Test
    void createViewWithNestedCteWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE VIEW v AS WITH C1 AS (WITH C2 AS (SELECT foo_field, id, baz_field FROM bar where id > 20) SELECT * FROM C2) SELECT * FROM C1 " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) ";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var viewMaybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v"));
                assertThat(viewMaybe).isPresent();
                assertThat(Assert.optionalUnchecked(viewMaybe).getDescription()).isEqualTo("WITH C1 AS (WITH C2 AS (SELECT foo_field, id, baz_field FROM bar where id > 20) SELECT * FROM C2) SELECT * FROM C1");
                return txn -> {
                };
            }
        });
    }

    @Test
    void createViewWithFunctionAndCteComplexNestingWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE FUNCTION F1 (IN A BIGINT) AS SELECT id, baz_field, foo_field FROM bar WHERE id > A " +
                "CREATE VIEW v AS WITH C1 AS (WITH C2 AS (SELECT foo_field, id, baz_field FROM F1(20)) SELECT * FROM C2) SELECT * FROM C1 ";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var viewMaybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v"));
                assertThat(viewMaybe).isPresent();
                assertThat(Assert.optionalUnchecked(viewMaybe).getDescription()).isEqualTo("WITH C1 AS (WITH C2 AS (SELECT foo_field, id, baz_field FROM F1(20)) SELECT * FROM C2) SELECT * FROM C1");
                return txn -> {
                };
            }
        });
    }

    // This will be resolved once https://github.com/FoundationDB/fdb-record-layer/issues/3493 is fixed.
    @Test
    void createViewWithFunctionAndCteReferencingAnotherViewDoesNotWork() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE VIEW p AS SELECT * FROM bar " +
                "CREATE FUNCTION F1 (IN A BIGINT) AS SELECT id, baz_field, foo_field FROM p WHERE id > A " +
                "CREATE VIEW v AS WITH C1 AS (WITH C2 AS (SELECT foo_field, id, baz_field FROM F1(20)) SELECT * FROM C2) SELECT * FROM C1 " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) ";

        shouldFailWithInjectedQueryFactory(schemaStatement, ErrorCode.UNDEFINED_TABLE, new AbstractQueryFactory() {
            @Override
            public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
                Assertions.fail("Should not call the query!");
                return DdlQuery.NoOpDdlQuery.INSTANCE;
            }
        });
    }

    @Test
    void createViewWithReferencesToSubsequentlyDefinedTableAndTypeWorks() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE VIEW v AS SELECT * FROM bar " +
                "CREATE TABLE bar (id bigint, baz_field baz, foo_field foo, PRIMARY KEY(id)) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) ";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var viewMaybe = Assertions.assertDoesNotThrow(() -> template.findViewByName("v"));
                assertThat(viewMaybe).isPresent();
                assertThat(Assert.optionalUnchecked(viewMaybe).getDescription()).isEqualTo("SELECT * FROM bar");
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexOnBasicSyntax() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, a bigint, b bigint, c bigint, PRIMARY KEY(id)) " +
                "CREATE INDEX i1 on bar(a, b) include (c)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("bar"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(Key.Expressions.keyWithValue(
                        Key.Expressions.concat(Key.Expressions.field("a"), Key.Expressions.field("b"), Key.Expressions.field("c")), 2));
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexOnPredicatedView() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, a bigint, b bigint, c bigint, PRIMARY KEY(id)) " +
                "CREATE VIEW v1 AS SELECT b, c FROM bar WHERE a < 100 " +
                "CREATE INDEX i1 on v1(b, c)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("bar"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.concat(Key.Expressions.field("b"), Key.Expressions.field("c")));
                assertThat(recordLayerIndex.getPredicate()).isNotNull();
                final var predicate = Assert.notNullUnchecked(recordLayerIndex.getPredicate());
                final var expectedPredicateProto = RecordMetaDataProto.Predicate.newBuilder()
                        .setValuePredicate(RecordMetaDataProto.ValuePredicate.newBuilder().addValue("a")
                                .setComparison(RecordMetaDataProto.Comparison.newBuilder()
                                        .setSimpleComparison(RecordMetaDataProto.SimpleComparison.newBuilder()
                                                .setType(RecordMetaDataProto.ComparisonType.LESS_THAN)
                                                .setOperand(RecordKeyExpressionProto.Value.newBuilder().setLongValue(100L).build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                assertThat(predicate).isEqualTo(expectedPredicateProto);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createVectorIndex() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, a bigint, b bigint, c bigint, PRIMARY KEY(id)) " +
                "CREATE VIEW v1 AS SELECT b, c FROM bar WHERE a < 100 " +
                "CREATE INDEX i1 on v1(b, c)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("bar"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.concat(Key.Expressions.field("b"), Key.Expressions.field("c")));
                assertThat(recordLayerIndex.getPredicate()).isNotNull();
                final var predicate = Assert.notNullUnchecked(recordLayerIndex.getPredicate());
                final var expectedPredicateProto = RecordMetaDataProto.Predicate.newBuilder()
                        .setValuePredicate(RecordMetaDataProto.ValuePredicate.newBuilder().addValue("a")
                                .setComparison(RecordMetaDataProto.Comparison.newBuilder()
                                        .setSimpleComparison(RecordMetaDataProto.SimpleComparison.newBuilder()
                                                .setType(RecordMetaDataProto.ComparisonType.LESS_THAN)
                                                .setOperand(RecordKeyExpressionProto.Value.newBuilder().setLongValue(100L).build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                assertThat(predicate).isEqualTo(expectedPredicateProto);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexOnBasicSyntaxComplex() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT baz (a bigint, b bigint) " +
                "CREATE TYPE AS ENUM foo ('OPTION_1', 'OPTION_2') " +
                "CREATE TABLE bar (id bigint, a bigint, b bigint, c bigint, PRIMARY KEY(id)) " +
                "CREATE VIEW v1 AS SELECT * FROM (SELECT b as x, c as y FROM bar) as d " +
                "CREATE INDEX i1 on v1(x desc nulls first, y asc nulls last)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("bar"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.concat(Key.Expressions.function("order_desc_nulls_first", Key.Expressions.field("b")),
                                Key.Expressions.function("order_asc_nulls_last", Key.Expressions.field("c"))));
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexOnRepeated() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p)) " +
                "CREATE VIEW mv1 AS SELECT SQ.x, t.p from T AS t, (select M.x from t.a AS M) SQ " +
                "CREATE INDEX i1 on mv1(x, p)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.concat(Key.Expressions.field("a", KeyExpression.FanType.None)
                                .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest("x")), Key.Expressions.field("p")));
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexOnRepeatedUsingMatViewSyntax() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.x, t.p from T AS t, (select M.x from t.a AS M) SQ order by SQ.x, t.p ";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
                assertThat(index.getName()).isEqualTo("mv1");
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.concat(Key.Expressions.field("a", KeyExpression.FanType.None)
                                .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut).nest("x")), Key.Expressions.field("p")));
                return txn -> {
                };
            }
        });
    }


    @Test
    void createIndexOnAggregate() throws Exception {
        final String schemaStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, a bigint, b bigint, c bigint, primary key(p)) " +
                "CREATE VIEW mv1 AS SELECT sum(c) as S, a, b from T group by a, b " +
                "CREATE INDEX i1 on mv1(a, b) include (S)";

        shouldWorkWithInjectedFactory(schemaStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
                assertThat(tableMaybe).isPresent();
                final var table = Assert.optionalUnchecked(tableMaybe);
                assertThat(table.getIndexes().size()).isEqualTo(1);
                final var index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                assertThat(index.getIndexType()).isEqualTo(IndexTypes.SUM);
                assertThat(index.getName()).isEqualTo("i1");
                assertThat(index).isInstanceOf(RecordLayerIndex.class);
                final var recordLayerIndex = Assert.castUnchecked(index, RecordLayerIndex.class);
                assertThat(recordLayerIndex.getKeyExpression()).isEqualTo(
                        Key.Expressions.field("c").groupBy(Key.Expressions.concat(Key.Expressions.field("a"), Key.Expressions.field("b"))));
                return txn -> {
                };
            }
        });
    }

    @Nonnull
    private static String makeColumnDefinition(@Nonnull final List<String> columns, boolean isTable) {
        StringBuilder columnStatement = new StringBuilder("(");
        int pos = 0;
        for (String col : columns) {
            if (pos != 0) {
                columnStatement.append(",");
            }
            columnStatement.append("col").append(pos).append(" ").append(col);
            pos++;
        }
        if (isTable) {
            //now add a primary key
            columnStatement.append(", PRIMARY KEY(col0)");
        }
        return columnStatement.append(")").toString();
    }

    @Nonnull
    private static List<String> chooseIndexColumns(@Nonnull final List<String> columns, @Nonnull final IntPredicate indexChoice) {
        //choose every other column
        return IntStream.range(0, columns.size())
                .filter(indexChoice)
                .mapToObj(n -> "col" + n)
                .collect(Collectors.toList());
    }

    private static void assertColumnsMatch(@Nonnull final DdlTestUtil.ParsedType type, @Nonnull final List<String> expectedColumns) {
        Assertions.assertNotNull(type, "No type found!");
        List<String> columnStrings = type.getColumnStrings();
        List<String> expectedColStrings = IntStream.range(0, expectedColumns.size())
                .mapToObj(i -> ("col" + i + " " + expectedColumns.get(i)))
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedColStrings, columnStrings, "Incorrect columns for type <" + type.getName() + ">");
    }

    private static void checkColumnNullability(@Nonnull final SchemaTemplate template, int sqlType, boolean isNullable) {
        Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
        Assertions.assertEquals(1, ((RecordLayerSchemaTemplate) template).getTables().size(), "should have only 1 table");
        final var table = ((RecordLayerSchemaTemplate) template).findTableByName("bar");
        Assertions.assertTrue(table.isPresent());
        final var columns = table.get().getColumns();
        Assertions.assertEquals(2, columns.size());
        final var maybeNullableArrayColumn = columns.stream().filter(c -> c.getName().equals("foo_field")).findFirst();
        Assertions.assertTrue(maybeNullableArrayColumn.isPresent());
        if (isNullable) {
            Assertions.assertTrue(maybeNullableArrayColumn.get().getDataType().isNullable());
        } else {
            Assertions.assertFalse(maybeNullableArrayColumn.get().getDataType().isNullable());
        }
        Assertions.assertEquals(sqlType, maybeNullableArrayColumn.get().getDataType().getJdbcSqlCode());
    }

    @Nonnull
    private static String replaceLast(@Nonnull final String str, final char oldChar, @Nonnull final String replacement) {
        if (str.isEmpty()) {
            return str;
        }

        int lastIndex = str.lastIndexOf(oldChar);
        if (lastIndex == -1) {
            return str;
        }

        return str.substring(0, lastIndex) + replacement + str.substring(lastIndex + 1);
    }
}
