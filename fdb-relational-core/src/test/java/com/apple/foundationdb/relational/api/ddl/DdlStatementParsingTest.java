/*
 * DdlStatementParsingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.utils.PermutationIterator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests that verify that the language behaves correctly and has nice features and stuff. It does _not_ verify
 * that the underlying execution is correct, only that the language is parsed as expected.
 */
@API(API.Status.EXPERIMENTAL)
public class DdlStatementParsingTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, DdlStatementParsingTest.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    private static final String[] validPrimitiveDataTypes = new String[]{
            "integer", "bigint", "double", "boolean", "string", "bytes"
    };

    public DdlStatementParsingTest() throws RelationalException, SQLException {
    }

    private PlanContext getFakePlanContext() throws SQLException, RelationalException {
        final var embeddedConnection = connection.getUnderlying().unwrap(EmbeddedRelationalConnection.class);
        final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder().setVersion(1).setName(database.getSchemaTemplateName()).build();
        RecordMetaDataProto.MetaData md = schemaTemplate.toRecordMetadata().toProto();
        return PlanContext.Builder.create()
                .withMetadata(RecordMetaData.build(md))
                .withMetricsCollector(embeddedConnection.getMetricCollector())
                .withPlannerConfiguration(PlannerConfiguration.ofAllAvailableIndexes())
                .withUserVersion(0)
                .withDbUri(URI.create("/DdlStatementParsingTest"))
                .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                .withConstantActionFactory(NoOpMetadataOperationsFactory.INSTANCE)
                .withSchemaTemplate(schemaTemplate)
                .build();
    }

    private PlanGenerator getPlanGenerator(@Nonnull PlanContext planContext) throws SQLException, RelationalException {
        final var embeddedConnection = connection.getUnderlying().unwrap(EmbeddedRelationalConnection.class);
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final var storeState = new RecordStoreState(null, Map.of());
        final FDBRecordStoreBase<?> store = database.loadSchema(connection.getSchema()).loadStore().unwrap(FDBRecordStoreBase.class);
        return PlanGenerator.of(Optional.empty(), planContext, store.getRecordMetaData(), storeState, Options.NONE);
    }

    public static Stream<Arguments> columnTypePermutations() {
        int numColumns = 2;
        final List<String> items = List.of(validPrimitiveDataTypes);

        final PermutationIterator<String> permutations = PermutationIterator.generatePermutations(items, numColumns);
        return permutations.stream().map(Arguments::of);
    }

    void shouldFailWith(@Nonnull final String query, @Nullable ErrorCode errorCode) throws Exception {
        connection.setAutoCommit(false);
        ((EmbeddedRelationalConnection) connection.getUnderlying()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                getPlanGenerator(PlanContext.Builder.unapply(getFakePlanContext()).build()).getPlan(query));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldFailWithInjectedFactory(@Nonnull final String query, @Nullable ErrorCode errorCode, @Nonnull MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        connection.setAutoCommit(false);
        ((EmbeddedRelationalConnection) connection.getUnderlying()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                getPlanGenerator(PlanContext.Builder.unapply(getFakePlanContext()).withConstantActionFactory(metadataOperationsFactory).build()).getPlan(query));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        connection.setAutoCommit(false);
        ((EmbeddedRelationalConnection) connection.getUnderlying()).createNewTransaction();
        getPlanGenerator(PlanContext.Builder.unapply(getFakePlanContext()).withConstantActionFactory(metadataOperationsFactory).build()).getPlan(query);
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldFailWithInjectedQueryFactory(@Nonnull final String query, @Nullable ErrorCode errorCode, @Nonnull DdlQueryFactory queryFactory) throws Exception {
        connection.setAutoCommit(false);
        ((EmbeddedRelationalConnection) connection.getUnderlying()).createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                getPlanGenerator(PlanContext.Builder.unapply(getFakePlanContext()).withDdlQueryFactory(queryFactory).build()).getPlan(query));
        connection.rollback();
        connection.setAutoCommit(true);
        Assertions.assertEquals(errorCode, ve.getErrorCode());
    }

    void shouldWorkWithInjectedQueryFactory(@Nonnull final String query, @Nonnull DdlQueryFactory queryFactory) throws Exception {
        connection.setAutoCommit(false);
        ((EmbeddedRelationalConnection) connection.getUnderlying()).createNewTransaction();
        getPlanGenerator(PlanContext.Builder.unapply(getFakePlanContext()).withDdlQueryFactory(queryFactory).build()).getPlan(query);
        connection.rollback();
        connection.setAutoCommit(true);
    }

    @Nonnull
    private static DescriptorProtos.FileDescriptorProto getProtoDescriptor(@Nonnull final SchemaTemplate schemaTemplate) {
        Assertions.assertTrue(schemaTemplate instanceof RecordLayerSchemaTemplate);
        final var asRecordLayerSchemaTemplate = (RecordLayerSchemaTemplate) schemaTemplate;
        return asRecordLayerSchemaTemplate.toRecordMetadata().toProto().getRecords();
    }

    @Test
    void indexFailsWithNonExistingTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE INDEX t_idx as select a from foo";
        shouldFailWith(stmt, ErrorCode.INVALID_SCHEMA_TEMPLATE);
    }

    @Test
    void indexFailsWithNonExistingIndexColumn() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE foo(a bigint, PRIMARY KEY(a))" +
                " CREATE INDEX t_idx as select non_existing from foo";
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN);
    }

    @Test
    void indexFailsWithReservedKeywordAsName() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE INDEX table as select a from foo";
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
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                Assertions.assertTrue(template instanceof RecordLayerSchemaTemplate);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate) template).getTables().size(), "should have only 1 table");
                DescriptorProtos.FileDescriptorProto fileDescriptorProto = getProtoDescriptor(template);
                Assertions.assertEquals(1, fileDescriptorProto.getEnumTypeCount(), "should have one enum defined");
                fileDescriptorProto.getEnumTypeList().forEach(enumDescriptorProto -> {
                    Assertions.assertEquals("MY_ENUM", enumDescriptorProto.getName());
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

    @Test
    void failsToParseEmptyTemplateStatements() throws Exception {
        //empty template statements are invalid, and can be rejected in the parser
        final String stmt = "CREATE SCHEMA TEMPLATE test_template ";
        boolean[] visited = new boolean[]{false};
        shouldFailWithInjectedFactory(stmt, ErrorCode.SYNTAX_ERROR, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertTrue(template instanceof RecordLayerSchemaTemplate);
                Assertions.assertEquals(0, ((RecordLayerSchemaTemplate) template).getTables().size(), "Tables defined!");
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
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.fail("Should fail during parsing!");
                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithOutOfOrderDefinitionsWork(List<String> columns) throws Exception {
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) +
                "CREATE TYPE AS STRUCT FOO " + makeColumnDefinition(columns, false) +
                "";

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertTrue(template instanceof RecordLayerSchemaTemplate);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate) template).getTables().size(), "Incorrect number of tables");
                return txn -> {
                };
            }
        });
    }

    /*Schema Template tests*/
    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplates(List<String> columns) throws Exception {
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TYPE AS STRUCT FOO " + makeColumnDefinition(columns, false) +
                " CREATE TABLE BAR (col0 bigint, col1 FOO, PRIMARY KEY(col0))";
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("TEST_TEMPLATE", template.getName(), "incorrect template name!");
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
    void createSchemaTemplateTableWithOnlyRecordType(List<String> columns) throws Exception {
        final String baseTableDef = makeColumnDefinition(columns, false).replace(")", ", SINGLE ROW ONLY)");
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template  " +
                "CREATE TABLE FOO " + baseTableDef;

        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("TEST_TEMPLATE", template.getName(), "incorrect template name!");
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
    void createSchemaTemplateWithDuplicateIndexesFails(List<String> columns) throws Exception {
        final String baseTableDef = makeColumnDefinition(columns, true);
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE FOO " + baseTableDef +
                " CREATE INDEX foo_idx as select col0 from foo order by col0" +
                " CREATE INDEX foo_idx as select col1 from foo order by col1"; //duplicate with the same name  on same table should fail

        shouldFailWithInjectedFactory(columnStatement, ErrorCode.INDEX_ALREADY_EXISTS, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.fail("Should not call this!");
                return txn -> {
                };
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplateWithIndex(List<String> columns) throws Exception {
        final String indexColumns = String.join(",", chooseIndexColumns(columns, n -> n % 2 == 0));
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template  " +
                "CREATE TYPE AS STRUCT FOO " + makeColumnDefinition(columns, false) +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) +
                "CREATE INDEX v_idx as select " + indexColumns + " from tbl order by " + indexColumns;

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertTrue(template instanceof RecordLayerSchemaTemplate);
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate) template).getTables().size(), "Incorrect number of tables");
                Table info = ((RecordLayerSchemaTemplate) template).getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final Index index = info.getIndexes().stream().findFirst().get();
                Assertions.assertEquals("V_IDX", index.getName(), "Incorrect index name!");

                final var actualKe = ((RecordLayerIndex) index).getKeyExpression().toKeyExpression();
                List<RecordMetaDataProto.KeyExpression> keys = null;
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
    void createSchemaTemplateWithIndexAndInclude(List<String> columns) throws Exception {
        Assumptions.assumeTrue(columns.size() > 1); //the test only works with multiple columns
        final List<String> indexedColumns = chooseIndexColumns(columns, n -> n % 2 == 0); //choose every other column
        final List<String> unindexedColumns = chooseIndexColumns(columns, n -> n % 2 != 0);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TYPE AS STRUCT FOO " + makeColumnDefinition(columns, false) +
                " CREATE TABLE TBL " + makeColumnDefinition(columns, true) +
                " CREATE INDEX v_idx as select " + Stream.concat(indexedColumns.stream(), unindexedColumns.stream()).collect(Collectors.joining(",")) + " from tbl order by " + String.join(",", indexedColumns);
        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, ((RecordLayerSchemaTemplate) template).getTables().size(), "Incorrect number of tables");
                Table info = ((RecordLayerSchemaTemplate) template).getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final Index index = info.getIndexes().stream().findFirst().get();
                Assertions.assertEquals("V_IDX", index.getName(), "Incorrect index name!");

                RecordMetaDataProto.KeyExpression actualKe = ((RecordLayerIndex) index).getKeyExpression().toKeyExpression();
                Assertions.assertNotNull(actualKe.getKeyWithValue(), "Null KeyExpression for included columns!");
                final RecordMetaDataProto.KeyWithValue keyWithValue = actualKe.getKeyWithValue();

                //This is a weird workaround for the problem fixed in https://github.com/FoundationDB/fdb-record-layer/pull/1585,
                // once that's been merged and we get a release that contains it, we can replace this with a more
                //natural api
                final RecordMetaDataProto.KeyExpression innerKey = keyWithValue.getInnerKey();
                int splitPoint = keyWithValue.getSplitPoint();
                final ThenKeyExpression then = new ThenKeyExpression(innerKey.getThen());
                KeyExpression keyExpr = then.getSubKey(0, splitPoint);
                KeyExpression valueExpr = then.getSubKey(splitPoint, then.getColumnSize());

                Assertions.assertEquals(indexedColumns.size(), keyExpr.getColumnSize(), "Incorrect number of parsed columns!");
                for (int i = 0; i < indexedColumns.size(); i++) {
                    final RecordMetaDataProto.KeyExpression ke = keyExpr.getSubKey(i, i + 1).toKeyExpression();
                    Assertions.assertEquals(indexedColumns.get(i), ke.getField().getFieldName(), "Incorrect column at position " + i);
                }

                Assertions.assertEquals(unindexedColumns.size(), valueExpr.getColumnSize(), "Incorrect number of parsed columns!");
                for (int i = 0; i < unindexedColumns.size(); i++) {
                    final RecordMetaDataProto.KeyExpression ve = valueExpr.getSubKey(i, i + 1).toKeyExpression();
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
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
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
        boolean[] called = new boolean[]{false};
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, boolean throwIfDoesNotExist, @Nonnull Options options) {
                Assertions.assertEquals("TEST_TEMPLATE", templateId, "Incorrect schema template name!");
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
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
                Assertions.fail("Should fail with a parser error");
                return super.getCreateSchemaTemplateConstantAction(template, templateProperties);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createTable(List<String> columns) throws Exception {
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template CREATE TABLE FOO " +
                makeColumnDefinition(columns, true);
        shouldWorkWithInjectedFactory(columnStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("TEST_TEMPLATE", template.getName(), "incorrect template name!");
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
    void createTableAndType(List<String> columns) throws Exception {
        final String tableDef = "CREATE TABLE tbl " + makeColumnDefinition(columns, true);
        final String typeDef = "CREATE TYPE AS STRUCT typ " + makeColumnDefinition(columns, false);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template " +
                typeDef + " " +
                tableDef;

        shouldWorkWithInjectedFactory(templateStatement, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("TEST_TEMPLATE", template.getName(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(getProtoDescriptor(template));
                Assertions.assertEquals(1, schema.getTables().size(), "Incorrect number of tables");
                return txn -> {
                    try {
                        assertColumnsMatch(schema.getTable("tbl"), columns);
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
                Assertions.assertEquals(URI.create("/DB_PATH"), dbPath, "Incorrect database path!");
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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

        boolean[] called = new boolean[]{false};
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
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                return txn -> {
                };
            }
        });
    }

    private String makeColumnDefinition(List<String> columns, boolean isTable) {
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

    private List<String> chooseIndexColumns(List<String> columns, IntPredicate indexChoice) {
        //choose every other column
        return IntStream.range(0, columns.size())
                .filter(indexChoice)
                .mapToObj(n -> "COL" + n)
                .collect(Collectors.toList());
    }

    private void assertColumnsMatch(DdlTestUtil.ParsedType type, List<String> expectedColumns) {
        Assertions.assertNotNull(type, "No type found!");
        List<String> columnStrings = type.getColumnStrings();
        List<String> expectedColStrings = IntStream.range(0, expectedColumns.size())
                .mapToObj(i -> ("col" + i + " " + expectedColumns.get(i)))
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedColStrings, columnStrings, "Incorrect columns for type <" + type.getName() + ">");
    }
}
