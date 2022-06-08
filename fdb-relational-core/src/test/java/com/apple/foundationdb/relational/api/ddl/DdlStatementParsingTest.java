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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.TableInfo;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpConstantActionFactory;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.utils.PermutationIterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Tests that verify that the language behaves correctly and has nice features and stuff. It does _not_ verify
 * that the underlying execution is correct, only that the language is parsed as expected.
 */
public class DdlStatementParsingTest {

    private final PlanContext fakePlanContext;

    private static final String[] validPrimitiveDataTypes = new String[]{
            "int64", "double", "boolean", "string", "bytes"
    };

    public DdlStatementParsingTest() throws RelationalException {
        TypingContext ctx = TypingContext.create();
        SystemTableRegistry.getSystemTable("SCHEMAS").addDefinition(ctx);
        SystemTableRegistry.getSystemTable("DATABASES").addDefinition(ctx);
        ctx.addAllToTypeRepository();
        RecordMetaDataProto.MetaData md = ctx.generateSchemaTemplate("catalog_template").generateSchema("__SYS", "catalog").getMetaData();
        fakePlanContext = PlanContext.Builder.create()
                .withMetadata(RecordMetaData.build(md))
                .withStoreState(new RecordStoreState(RecordMetaDataProto.DataStoreInfo.newBuilder().build(), null))
                .withDbUri(URI.create("/DdlStatementParsingTest"))
                .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                .withConstantActionFactory(NoOpConstantActionFactory.INSTANCE)
                .build();
    }

    public static Stream<Arguments> columnTypePermutations() {
        int numColumns = 2;
        final List<String> items = List.of(validPrimitiveDataTypes);

        final PermutationIterator<String> permutations = PermutationIterator.generatePermutations(items, numColumns);
        return permutations.stream().map(Arguments::of);
    }

    void shouldFailWith(@Nonnull final String query, @Nullable ErrorCode errorCode) throws Exception {
        shouldFailWithInjectedFactory(query, errorCode, fakePlanContext.getConstantActionFactory());
    }

    void shouldFailWithInjectedFactory(@Nonnull final String query, @Nullable ErrorCode errorCode, @Nonnull ConstantActionFactory constantActionFactory) throws Exception {
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withConstantActionFactory(constantActionFactory).build()));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull ConstantActionFactory constantActionFactory) throws Exception {
        Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withConstantActionFactory(constantActionFactory).build());
    }

    void shouldFailWithInjectedQueryFactory(@Nonnull final String query, @Nullable ErrorCode errorCode, @Nonnull DdlQueryFactory queryFactory) throws Exception {
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withDdlQueryFactory(queryFactory).build()));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
    }

    void shouldWorkWithInjectedQueryFactory(@Nonnull final String query, @Nonnull DdlQueryFactory queryFactory) throws Exception {
        Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withDdlQueryFactory(queryFactory).build());
    }

    @Test
    void indexFailsWithNonExistingTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template as {" +
                "CREATE VALUE INDEX t_idx on foo(a);" +
                "}";
        shouldFailWith(stmt, ErrorCode.UNKNOWN_TYPE);
    }

    @Test
    void indexFailsWithNonExistingIndexColumn() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template as {" +
                "CREATE TABLE foo(a int64);" +
                "CREATE VALUE INDEX t_idx on foo(NON_EXISTING);" +
                "}";
        shouldFailWith(stmt, ErrorCode.UNKNOWN_FIELD);
    }

    @Test
    void indexFailsWithReservedKeywordAsName() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template as {" +
                "CREATE VALUE INDEX table on foo(a);" +
                "}";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void failsToParseEmptyTemplateStatements() throws Exception {
        //empty template statements are invalid, and can be rejected in the parser
        final String stmt = "CREATE SCHEMA TEMPLATE test_template AS {;}";
        boolean[] visited = new boolean[]{false};
        shouldFailWithInjectedFactory(stmt, ErrorCode.SYNTAX_ERROR, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(0, template.getTables().size(), "Tables defined!");
                Assertions.assertEquals(0, template.getTypes().size(), "Tables defined!");
                visited[0] = true;
                return txn -> {
                };
            }
        });
        Assertions.assertFalse(visited[0], "called for a constant action!");
    }

    @Test
    void createTypeWithPrimaryKeyFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template as {" +
                "CREATE STRUCT t (a int64, b string PRIMARY KEY(b));" +
                "}";
        shouldFailWithInjectedFactory(stmt, ErrorCode.SYNTAX_ERROR, new AbstractConstantActionFactory() {
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
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                // index references a table that is not seen yet.
                "CREATE VALUE INDEX v_idx on TBL(" + String.join(",", chooseIndexColumns(columns, n -> n % 2 == 0)) + ");" +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) + ";" +
                "CREATE STRUCT FOO " + makeColumnDefinition(columns, false) + ";" +
                "}";

        shouldWorkWithInjectedFactory(templateStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, template.getTables().size(), "Incorrect number of tables");
                TableInfo info = template.getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final RecordMetaDataProto.Index index = info.getIndexes().get(0);
                Assertions.assertEquals("v_idx", index.getName(), "Incorrect index name!");

                RecordMetaDataProto.KeyExpression actualKe = index.getRootExpression();
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

    /*Schema Template tests*/
    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void createSchemaTemplates(List<String> columns) throws Exception {
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                "CREATE STRUCT FOO " + makeColumnDefinition(columns, false) +
                "}";
        shouldWorkWithInjectedFactory(columnStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getUniqueId(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(template.toProtobufDescriptor());
                Assertions.assertEquals(0, schema.getTables().size(), "Incorrect number of tables");
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
        final String baseTableDef = makeColumnDefinition(columns, false).replace(")", " PRIMARY KEY(RECORD TYPE))");
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                "CREATE TABLE FOO " + baseTableDef +
                "}";

        shouldWorkWithInjectedFactory(columnStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getUniqueId(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(template.toProtobufDescriptor());
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
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                "CREATE TABLE FOO " + baseTableDef +
                "; CREATE VALUE INDEX foo_idx on FOO(col0)" +
                "; CREATE VALUE INDEX foo_idx on FOO(col1)" //duplicate with the same name  on same table should fail
                + "}";

        shouldFailWithInjectedFactory(columnStatement, ErrorCode.INDEX_EXISTS, new AbstractConstantActionFactory() {
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
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                "CREATE STRUCT FOO " + makeColumnDefinition(columns, false) + ";" +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) + ";" +
                "CREATE VALUE INDEX v_idx on TBL(" + String.join(",", chooseIndexColumns(columns, n -> n % 2 == 0)) + ");" +
                "}";

        shouldWorkWithInjectedFactory(templateStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, template.getTables().size(), "Incorrect number of tables");
                TableInfo info = template.getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final RecordMetaDataProto.Index index = info.getIndexes().get(0);
                Assertions.assertEquals("v_idx", index.getName(), "Incorrect index name!");

                RecordMetaDataProto.KeyExpression actualKe = index.getRootExpression();
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
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template AS { " +
                "CREATE STRUCT FOO " + makeColumnDefinition(columns, false) + ";" +
                "CREATE TABLE TBL " + makeColumnDefinition(columns, true) + ";" +
                "CREATE VALUE INDEX v_idx on TBL(" + String.join(",", indexedColumns) + ") INCLUDE (" + String.join(",", unindexedColumns) + ");" +
                "}";
        shouldWorkWithInjectedFactory(templateStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, template.getTables().size(), "Incorrect number of tables");
                TableInfo info = template.getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final RecordMetaDataProto.Index index = info.getIndexes().get(0);
                Assertions.assertEquals("v_idx", index.getName(), "Incorrect index name!");

                RecordMetaDataProto.KeyExpression actualKe = index.getRootExpression();
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

                //account for the record type key
                Assertions.assertEquals(indexedColumns.size(), keyExpr.getColumnSize() - 1, "Incorrect number of parsed columns!");
                for (int i = 1; i <= indexedColumns.size(); i++) {
                    final RecordMetaDataProto.KeyExpression ke = keyExpr.getSubKey(i, i + 1).toKeyExpression();
                    Assertions.assertEquals(indexedColumns.get(i - 1), ke.getField().getFieldName(), "Incorrect column at position " + (i - 1));
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

    @Test
    void dropSchemaTemplates() throws Exception {
        final String columnStatement = "DROP SCHEMA TEMPLATE test_template";
        boolean[] called = new boolean[]{false};
        shouldWorkWithInjectedFactory(columnStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, @Nonnull Options options) {
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
        final String command = "CREATE SCHEMA TEMPLATE no_types AS {}"; // parser rules design doesn't permit this case.

        shouldFailWithInjectedFactory(command, ErrorCode.SYNTAX_ERROR, new AbstractConstantActionFactory() {
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
        final String columnStatement = "CREATE SCHEMA TEMPLATE test_template AS { CREATE TABLE FOO " +
                makeColumnDefinition(columns, true) +
                "}";
        shouldWorkWithInjectedFactory(columnStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getUniqueId(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(template.toProtobufDescriptor());
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
        final String typeDef = "CREATE STRUCT typ " + makeColumnDefinition(columns, false);
        final String templateStatement = "CREATE SCHEMA TEMPLATE test_template AS {" +
                typeDef + ";" +
                tableDef + ";" +
                "}";

        shouldWorkWithInjectedFactory(templateStatement, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals("test_template", template.getUniqueId(), "incorrect template name!");
                DdlTestUtil.ParsedSchema schema = new DdlTestUtil.ParsedSchema(template.toProtobufDescriptor());
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
        final String command = "CREATE DATABASE '/db_path'";

        shouldWorkWithInjectedFactory(command, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
                Assertions.assertEquals(URI.create("/db_path"), dbPath, "Incorrect database path!");
                return NoOpConstantActionFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath, constantActionOptions);
            }
        });
    }

    @Test
    void createDatabaseWithInvalidPathFails() throws Exception {
        final String command = "CREATE DATABASE not_a_path";

        shouldFailWithInjectedFactory(command, ErrorCode.INVALID_PATH, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
                Assertions.fail("We should not reach this point! We should throw a RelationalException instead");
                return NoOpConstantActionFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath, constantActionOptions);
            }
        });
    }

    @Test
    void dropDatabase() throws Exception {
        final String command = "DROP DATABASE '/db_path'";

        shouldWorkWithInjectedFactory(command, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, @Nonnull Options options) {
                Assertions.assertEquals(URI.create("/db_path"), dbUrl, "Incorrect database path!");
                return NoOpConstantActionFactory.INSTANCE.getDropDatabaseConstantAction(dbUrl, options);
            }
        });
    }

    @Test
    void dropDatabaseWithInvalidPathFails() throws Exception {
        final String command = "DROP DATABASE not_a_path";

        shouldFailWithInjectedFactory(command, ErrorCode.INVALID_PATH, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, @Nonnull Options options) {
                Assertions.fail("We should not reach this point! We should throw a RelationalException instead");
                return NoOpConstantActionFactory.INSTANCE.getCreateDatabaseConstantAction(dbUrl, options);
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
        final String command = "SHOW DATABASES WITH PREFIX '/prefix'";

        boolean[] called = new boolean[]{false};
        shouldWorkWithInjectedQueryFactory(command, new AbstractQueryFactory() {

            @Override
            public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
                called[0] = true;
                Assertions.assertNotNull(prefixPath, "Null URI passed!");
                Assertions.assertEquals(URI.create("/prefix"), prefixPath, "incorrect prefixed path specified!");
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
        final String templateName = "test_template";

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
        final String templateName = "test_template";

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
        final String templateName = "test_template";

        boolean[] called = new boolean[]{false};
        shouldWorkWithInjectedQueryFactory("DESCRIBE SCHEMA " + "'/test_db/" + templateName + "'", new AbstractQueryFactory() {
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
        final String templateName = "test_template";

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
        shouldWorkWithInjectedFactory("CREATE SCHEMA " + "'/test_db/" + templateName + "' WITH TEMPLATE " + templateName, new AbstractConstantActionFactory() {
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
            columnStatement.append(" PRIMARY KEY(col0)");
        }
        return columnStatement.append(")").toString();
    }

    private List<String> chooseIndexColumns(List<String> columns, IntPredicate indexChoice) {
        //choose every other column
        return IntStream.range(0, columns.size())
                .filter(indexChoice)
                .mapToObj(n -> "col" + n)
                .collect(Collectors.toList());
    }

    private void assertColumnsMatch(DdlTestUtil.ParsedType type, List<String> expectedColumns) {
        Assertions.assertNotNull(type, "No type found!");
        List<String> columnStrings = type.getColumnStrings();
        List<String> expectedColStrings = IntStream.range(0, expectedColumns.size())
                .mapToObj(i -> ("col" + i + " " + expectedColumns.get(i).toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
        Assertions.assertEquals(expectedColStrings, columnStrings, "Incorrect columns for type <" + type.getName() + ">");
    }
}
