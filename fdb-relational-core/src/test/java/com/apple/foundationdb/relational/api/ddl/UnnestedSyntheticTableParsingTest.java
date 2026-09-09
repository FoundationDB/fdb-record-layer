/*
 * UnnestedSyntheticTableParsingTest.java
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

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests the metadata produced by {@code CREATE INDEX} over an unnesting of an array.
 *
 * <p>Unnesting an array of structs defines the index on an unnested synthetic table, with one
 * nested constituent per array and key expressions rooted at a constituent alias. An array of scalars
 * does not warrant a synthetic table and keeps a fan-out key expression on the stored table.
 *
 * <p>Each scenario is spelled four ways — correlated subquery or PartiQL path, declared with
 * {@code INDEX ... ON <view>} or {@code INDEX ... AS SELECT} — and all four must agree.
 */
public class UnnestedSyntheticTableParsingTest {

    private static final String VIEW_SUBQUERY = "view + correlated subquery";
    private static final String VIEW_PARTIQL = "view + PartiQL path";
    private static final String AS_SELECT_SUBQUERY = "index as select + correlated subquery";
    private static final String AS_SELECT_PARTIQL = "index as select + PartiQL path";

    private static final String SINGLE_STRUCT_ARRAY_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, y bigint) " +
            "CREATE TABLE T(p bigint, a A array, primary key(p)) ";

    private static final String THREE_STRUCT_ARRAY_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, x2 bigint) CREATE TYPE AS STRUCT B(y bigint, y2 bigint) " +
            "CREATE TYPE AS STRUCT C(z bigint, z2 bigint) " +
            "CREATE TABLE T(p bigint, a A array, b B array, c C array, primary key(p)) ";

    private static final String SCALAR_ARRAY_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TABLE T(p bigint, s string array, primary key(p)) ";

    private static final String STRUCT_AND_SCALAR_ARRAY_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, y bigint) " +
            "CREATE TABLE T(p bigint, a A array, s string array, primary key(p)) ";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(UnnestedSyntheticTableParsingTest.class,
            TestSchemas.books(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build(), null);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA")
            .withOptions(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());

    public UnnestedSyntheticTableParsingTest() throws SQLException {
    }

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    /**
     * Plans the given DDL so that the assertions in the injected factory run against the resulting
     * schema template.
     *
     * @param query the DDL statement to plan
     * @param metadataOperationsFactory the factory holding the assertions
     * @throws Exception if planning or execution fails
     */
    void shouldWorkWithInjectedFactory(@Nonnull final String query,
                                       @Nonnull final MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        DdlTestUtil.shouldWorkWithInjectedFactory(connection, database.getSchemaTemplateName(),
                "/UnnestedSyntheticTableParsingTest", query, metadataOperationsFactory);
    }

    /**
     * Serializing is what registers the constituents on an {@code UnnestedRecordTypeBuilder}, so a constituent whose
     * parent or array field cannot be resolved only fails here. Asserting on the template alone misses that.
     */
    private static void assertSerializesWithSyntheticType(@Nonnull final SchemaTemplate template,
                                                          @Nonnull final String syntheticTableName) {
        final var metaData = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class).toRecordMetadata();
        assertThat(metaData.getSyntheticRecordTypes().containsKey(syntheticTableName)).isTrue();
    }

    // ─── A single unnested struct array ───────────────────────────────────────────────────────

    /**
     * Asserts the metadata for an index unnesting a single struct array.
     *
     * @param template the schema template that was built
     * @param indexName the name of the index that was declared
     */
    private static void assertUnnestedStructArrayIndex(@Nonnull final SchemaTemplate template,
                                                      @Nonnull final String indexName) {
        final String syntheticTableName = "__unnested_T_" + indexName;
        assertSerializesWithSyntheticType(template, syntheticTableName);

        final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
        assertThat(tableMaybe).isPresent();
        assertThat(Assert.optionalUnchecked(tableMaybe).getIndexes().size()).isEqualTo(0);

        final var syntheticTables = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                .getUnnestedSyntheticTables();
        assertThat(syntheticTables.size()).isEqualTo(1);
        final var syntheticTable = syntheticTables.stream().findFirst().orElseThrow();
        assertThat(syntheticTable.getName()).isEqualTo(syntheticTableName);
        assertThat(syntheticTable.getParentTableName()).isEqualTo("T");

        // One nested constituent for the unnested array, parented to the stored-record constituent.
        assertThat(syntheticTable.getConstituents().size()).isEqualTo(1);
        final var constituent = syntheticTable.getConstituents().get(0);
        assertThat(constituent.getNestingExpression()).isEqualTo(
                Key.Expressions.field("a").nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut)));
        assertThat(constituent.getParentAlias()).isEqualTo(syntheticTable.getAlias());

        assertThat(syntheticTable.getIndexes().size()).isEqualTo(1);
        final var index = syntheticTable.getIndexes().stream().findFirst().orElseThrow();
        assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
        assertThat(index.getName()).isEqualTo(indexName);
        assertThat(index.getTableName()).isEqualTo(syntheticTableName);
        // Constituent-alias paths, with no fan-out: the fan-out lives in the constituent's nesting
        // expression, and the ORDER BY column order is preserved.
        assertThat(index.getKeyExpression()).isEqualTo(
                Key.Expressions.concat(
                        Key.Expressions.field(constituent.getAlias(), KeyExpression.FanType.None).nest("x"),
                        Key.Expressions.field(syntheticTable.getAlias(), KeyExpression.FanType.None).nest("p"),
                        Key.Expressions.field(constituent.getAlias(), KeyExpression.FanType.None).nest("y")));
    }

    @Nonnull
    private AbstractMetadataOperationsFactory unnestedStructArrayIndexFactory(@Nonnull final String indexName) {
        return new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                assertUnnestedStructArrayIndex(template, indexName);
                return txn -> {
                };
            }
        };
    }

    @Nonnull
    private static Stream<Arguments> singleStructArraySpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "i1",
                        "CREATE VIEW mv1 AS SELECT SQ.x, t.p, SQ.y from T AS t, (select M.x, M.y from t.a AS M) SQ "
                                + "CREATE INDEX i1 on mv1(x, p, y)"),
                Arguments.of(VIEW_PARTIQL, "i1",
                        "CREATE VIEW mv1 AS SELECT M.x, t.p, M.y from T AS t, t.a AS M "
                                + "CREATE INDEX i1 on mv1(x, p, y)"),
                Arguments.of(AS_SELECT_SUBQUERY, "mv1",
                        "CREATE INDEX mv1 AS SELECT SQ.x, t.p, SQ.y from T AS t, (select M.x, M.y from t.a AS M) SQ "
                                + "order by SQ.x, t.p, SQ.y "),
                Arguments.of(AS_SELECT_PARTIQL, "mv1",
                        "CREATE INDEX mv1 AS SELECT M.x, t.p, M.y from T AS t, t.a AS M order by M.x, t.p, M.y "));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("singleStructArraySpellings")
    void createIndexOnRepeatedSplitReferencesUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                               @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(SINGLE_STRUCT_ARRAY_SCHEMA + indexDdl,
                unnestedStructArrayIndexFactory(indexName));
    }

    // ─── Multiple (3) unnested struct arrays ──────────────────────────────────────────────────

    /** Navigates to the elements of a nullable array, which is how the DDL layer stores {@code <T> array}. */
    @Nonnull
    private static KeyExpression wrappedArrayElements(@Nonnull final String arrayFieldName) {
        return Key.Expressions.field(arrayFieldName)
                .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut));
    }

    /**
     * Asserts the metadata for an index unnesting three struct arrays, one constituent per array.
     *
     * @param template the schema template that was built
     * @param indexName the name of the index that was declared
     */
    private static void assertThreeUnnestedStructArrayIndex(@Nonnull final SchemaTemplate template,
                                                            @Nonnull final String indexName) {
        final String syntheticTableName = "__unnested_T_" + indexName;
        assertSerializesWithSyntheticType(template, syntheticTableName);

        final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
        assertThat(tableMaybe).isPresent();
        assertThat(Assert.optionalUnchecked(tableMaybe).getIndexes().size()).isEqualTo(0);

        final var syntheticTables = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                .getUnnestedSyntheticTables();
        assertThat(syntheticTables.size()).isEqualTo(1);
        final var syntheticTable = syntheticTables.stream().findFirst().orElseThrow();
        assertThat(syntheticTable.getName()).isEqualTo(syntheticTableName);
        assertThat(syntheticTable.getParentTableName()).isEqualTo("T");

        // One constituent per unnested array, in declaration order, all parented to the stored record.
        final var constituents = syntheticTable.getConstituents();
        assertThat(constituents.size()).isEqualTo(3);
        assertThat(constituents.stream()
                .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getNestingExpression)
                .collect(Collectors.toList()))
                .isEqualTo(List.of(wrappedArrayElements("a"), wrappedArrayElements("b"), wrappedArrayElements("c")));
        constituents.forEach(c -> assertThat(c.getParentAlias()).isEqualTo(syntheticTable.getAlias()));

        assertThat(syntheticTable.getIndexes().size()).isEqualTo(1);
        final var index = syntheticTable.getIndexes().stream().findFirst().orElseThrow();
        assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
        assertThat(index.getName()).isEqualTo(indexName);
        assertThat(index.getTableName()).isEqualTo(syntheticTableName);
        assertThat(index.getKeyExpression()).isEqualTo(
                Key.Expressions.concat(
                        Key.Expressions.field(constituents.get(0).getAlias(), KeyExpression.FanType.None).nest("x"),
                        Key.Expressions.field(constituents.get(1).getAlias(), KeyExpression.FanType.None).nest("y"),
                        Key.Expressions.field(constituents.get(2).getAlias(), KeyExpression.FanType.None).nest("z"),
                        Key.Expressions.field(syntheticTable.getAlias(), KeyExpression.FanType.None).nest("p"),
                        Key.Expressions.field(constituents.get(0).getAlias(), KeyExpression.FanType.None).nest("x2"),
                        Key.Expressions.field(constituents.get(1).getAlias(), KeyExpression.FanType.None).nest("y2"),
                        Key.Expressions.field(constituents.get(2).getAlias(), KeyExpression.FanType.None).nest("z2")));
    }

    @Nonnull
    private AbstractMetadataOperationsFactory threeUnnestedStructArrayIndexFactory(@Nonnull final String indexName) {
        return new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                assertThreeUnnestedStructArrayIndex(template, indexName);
                return txn -> {
                };
            }
        };
    }

    @Nonnull
    private static Stream<Arguments> threeStructArraySpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "i1",
                        "CREATE VIEW v1 AS SELECT SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2 from T AS t, "
                                + "(select M.x, M.x2 from t.a AS M) SQ1, (select N.y, N.y2 from t.b AS N) SQ2, "
                                + "(select O.z, O.z2 from t.c AS O) SQ3 "
                                + "CREATE INDEX i1 on v1(x, y, z, p, x2, y2, z2)"),
                Arguments.of(VIEW_PARTIQL, "i1",
                        "CREATE VIEW v1 AS SELECT M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2 from T AS t, "
                                + "t.a AS M, t.b AS N, t.c AS O "
                                + "CREATE INDEX i1 on v1(x, y, z, p, x2, y2, z2)"),
                Arguments.of(AS_SELECT_SUBQUERY, "mv1",
                        "CREATE INDEX mv1 AS SELECT SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2 from T AS t, "
                                + "(select M.x, M.x2 from t.a AS M) SQ1, (select N.y, N.y2 from t.b AS N) SQ2, "
                                + "(select O.z, O.z2 from t.c AS O) SQ3 "
                                + "order by SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2"),
                Arguments.of(AS_SELECT_PARTIQL, "mv1",
                        "CREATE INDEX mv1 AS SELECT M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2 from T AS t, "
                                + "t.a AS M, t.b AS N, t.c AS O "
                                + "order by M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2"));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("threeStructArraySpellings")
    void createIndexOnMultipleRepeatedUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                                       @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(THREE_STRUCT_ARRAY_SCHEMA + indexDdl,
                threeUnnestedStructArrayIndexFactory(indexName));
    }

    // ─── Unnesting a scalar array ─────────────────────────────────────────────────────────────

    /**
     * Asserts the metadata for an index unnesting a scalar array: no synthetic table, and a fan-out key
     * expression on the stored table.
     *
     * @param template the schema template that was built
     * @param indexName the name of the index that was declared
     */
    private static void assertUnnestedScalarArrayIndex(@Nonnull final SchemaTemplate template,
                                                       @Nonnull final String indexName) {
        assertThat(Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                .getUnnestedSyntheticTables().size()).isEqualTo(0);

        final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
        assertThat(tableMaybe).isPresent();
        final var table = Assert.optionalUnchecked(tableMaybe);
        assertThat(table.getIndexes().size()).isEqualTo(1);
        final var index = Assert.castUnchecked(table.getIndexes().stream().findFirst().orElseThrow(),
                RecordLayerIndex.class);
        assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
        assertThat(index.getName()).isEqualTo(indexName);
        assertThat(index.getTableName()).isEqualTo("T");
        assertThat(index.getKeyExpression()).isEqualTo(
                Key.Expressions.concat(
                        Key.Expressions.field("s", KeyExpression.FanType.None)
                                .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut)),
                        Key.Expressions.field("p")));
    }

    @Nonnull
    private AbstractMetadataOperationsFactory unnestedScalarArrayIndexFactory(@Nonnull final String indexName) {
        return new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                assertUnnestedScalarArrayIndex(template, indexName);
                return txn -> {
                };
            }
        };
    }

    @Nonnull
    private static Stream<Arguments> scalarArraySpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "i1",
                        "CREATE VIEW v1 AS SELECT SQ.v, t.p from T AS t, (select v from t.s AS v) SQ "
                                + "CREATE INDEX i1 on v1(v, p)"),
                Arguments.of(VIEW_PARTIQL, "i1",
                        "CREATE VIEW v1 AS SELECT v, t.p from T AS t, t.s AS v "
                                + "CREATE INDEX i1 on v1(v, p)"),
                Arguments.of(AS_SELECT_SUBQUERY, "mv1",
                        "CREATE INDEX mv1 AS SELECT SQ.v, t.p from T AS t, (select v from t.s AS v) SQ "
                                + "order by SQ.v, t.p"),
                Arguments.of(AS_SELECT_PARTIQL, "mv1",
                        "CREATE INDEX mv1 AS SELECT v, t.p from T AS t, t.s AS v order by v, t.p"));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("scalarArraySpellings")
    void createIndexOnRepeatedScalarKeepsFanOut(@Nonnull final String spelling, @Nonnull final String indexName,
                                     @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(SCALAR_ARRAY_SCHEMA + indexDdl,
                unnestedScalarArrayIndexFactory(indexName));
    }

    // ─── Unnesting a struct array and a scalar array together ─────────────────────────────────

    /**
     * Asserts the metadata for an index unnesting a struct array and a scalar array: only the struct
     * array becomes a constituent, the scalar array fans out under the constituent that owns it.
     *
     * @param template the schema template that was built
     * @param indexName the name of the index that was declared
     */
    private static void assertUnnestedStructAndScalarArrayIndex(@Nonnull final SchemaTemplate template,
                                                                @Nonnull final String indexName) {
        final String syntheticTableName = "__unnested_T_" + indexName;
        assertSerializesWithSyntheticType(template, syntheticTableName);

        final var tableMaybe = Assertions.assertDoesNotThrow(() -> template.findTableByName("T"));
        assertThat(tableMaybe).isPresent();
        assertThat(Assert.optionalUnchecked(tableMaybe).getIndexes().size()).isEqualTo(0);

        final var syntheticTables = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                .getUnnestedSyntheticTables();
        assertThat(syntheticTables.size()).isEqualTo(1);
        final var syntheticTable = syntheticTables.stream().findFirst().orElseThrow();
        assertThat(syntheticTable.getName()).isEqualTo(syntheticTableName);

        // Only the struct array is a constituent; the scalar array is not.
        assertThat(syntheticTable.getConstituents().size()).isEqualTo(1);
        final var constituent = syntheticTable.getConstituents().get(0);
        assertThat(constituent.getNestingExpression()).isEqualTo(
                Key.Expressions.field("a").nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut)));
        assertThat(constituent.getParentAlias()).isEqualTo(syntheticTable.getAlias());

        assertThat(syntheticTable.getIndexes().size()).isEqualTo(1);
        final var index = syntheticTable.getIndexes().stream().findFirst().orElseThrow();
        assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
        assertThat(index.getName()).isEqualTo(indexName);
        assertThat(index.getTableName()).isEqualTo(syntheticTableName);
        // Struct element field via the constituent; scalar element via a fan-out under the parent.
        assertThat(index.getKeyExpression()).isEqualTo(
                Key.Expressions.concat(
                        Key.Expressions.field(constituent.getAlias(), KeyExpression.FanType.None).nest("x"),
                        Key.Expressions.field(syntheticTable.getAlias(), KeyExpression.FanType.None)
                                .nest(Key.Expressions.field("s", KeyExpression.FanType.None)
                                        .nest(Key.Expressions.field("values", KeyExpression.FanType.FanOut))),
                        Key.Expressions.field(constituent.getAlias(), KeyExpression.FanType.None).nest("y")));
    }

    @Nonnull
    private AbstractMetadataOperationsFactory unnestedStructAndScalarArrayIndexFactory(@Nonnull final String indexName) {
        return new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                assertUnnestedStructAndScalarArrayIndex(template, indexName);
                return txn -> {
                };
            }
        };
    }

    @Nonnull
    private static Stream<Arguments> structAndScalarArraySpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "i1",
                        "CREATE VIEW v1 AS SELECT SQ1.x, SQ2.v, SQ1.y from T AS t, "
                                + "(select M.x, M.y from t.a AS M) SQ1, (select v from t.s AS v) SQ2 "
                                + "CREATE INDEX i1 on v1(x, v, y)"),
                Arguments.of(VIEW_PARTIQL, "i1",
                        "CREATE VIEW v1 AS SELECT M.x, v, M.y from T AS t, t.a AS M, t.s AS v "
                                + "CREATE INDEX i1 on v1(x, v, y)"),
                Arguments.of(AS_SELECT_SUBQUERY, "mv1",
                        "CREATE INDEX mv1 AS SELECT SQ1.x, SQ2.v, SQ1.y from T AS t, "
                                + "(select M.x, M.y from t.a AS M) SQ1, (select v from t.s AS v) SQ2 "
                                + "order by SQ1.x, SQ2.v, SQ1.y"),
                Arguments.of(AS_SELECT_PARTIQL, "mv1",
                        "CREATE INDEX mv1 AS SELECT M.x, v, M.y from T AS t, t.a AS M, t.s AS v "
                                + "order by M.x, v, M.y"));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("structAndScalarArraySpellings")
    void createIndexOnRepeatedStructAndScalarUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                                              @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(STRUCT_AND_SCALAR_ARRAY_SCHEMA + indexDdl,
                unnestedStructAndScalarArrayIndexFactory(indexName));
    }

}
