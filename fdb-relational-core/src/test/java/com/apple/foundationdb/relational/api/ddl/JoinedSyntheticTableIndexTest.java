/*
 * JoinedSyntheticTableIndexTest.java
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerJoinedSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests the metadata produced by {@code CREATE INDEX} over a join of two or more tables.
 *
 * <p>An index whose columns come from several tables is defined on a joined synthetic record type, with one
 * inner-joined constituent per table and key expressions rooted at a constituent alias. The equalities relating
 * the tables become the synthetic table's join conditions rather than an index predicate.
 *
 * <p>Each scenario is spelled two ways — a comma join with the equality in {@code WHERE}, or an explicit
 * {@code INNER JOIN ... ON} — and both must produce identical metadata. They converge because an {@code ON}
 * condition is conjoined into the {@code WHERE} predicate while the query graph is built.
 *
 * <p>Constituent aliases are derived from the definition -- {@code joined_0}, {@code joined_1}, ... in the order the
 * joined tables were found -- rather than from the plan's correlations, so the same DDL always yields the same metadata
 * and the assertions can name them outright.
 */
public class JoinedSyntheticTableIndexTest {

    private static final String COMMA_JOIN = "comma join + WHERE";
    private static final String INNER_JOIN = "explicit INNER JOIN ... ON";
    private static final String USING_JOIN = "explicit INNER JOIN ... USING";
    private static final String VIEW_COMMA_JOIN = "view + comma join";
    private static final String VIEW_INNER_JOIN = "view + INNER JOIN ... ON";
    private static final String VIEW_USING_JOIN = "view + INNER JOIN ... USING";

    private static final String TWO_TABLE_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TABLE T1(k bigint, x bigint, primary key(k)) " +
            "CREATE TABLE T2(k bigint, y bigint, primary key(k)) ";

    private static final String THREE_TABLE_SCHEMA = TWO_TABLE_SCHEMA +
            "CREATE TABLE T3(k bigint, z bigint, primary key(k)) ";

    private static final String ARRAY_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(p bigint, q bigint) " +
            "CREATE TABLE T1(k bigint, x bigint, a A array, primary key(k)) " +
            "CREATE TABLE T2(k bigint, y bigint, primary key(k)) ";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(JoinedSyntheticTableIndexTest.class,
            TestSchemas.books(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build(), null);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA")
            .withOptions(Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());

    public JoinedSyntheticTableIndexTest() throws SQLException {
    }

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query,
                                       @Nonnull final MetadataOperationsFactory metadataOperationsFactory) throws Exception {
        DdlTestUtil.shouldWorkWithInjectedFactory(connection, database.getSchemaTemplateName(),
                "/JoinedSyntheticTableIndexTest", query, metadataOperationsFactory);
    }

    @Nonnull
    private static RecordLayerJoinedSyntheticTable soleJoinedTable(@Nonnull final SchemaTemplate template,
                                                                   @Nonnull final String indexName) {
        // No index is left on either stored table: the index belongs to the synthetic table.
        final var tables = Assertions.assertDoesNotThrow(template::getTables);
        tables.forEach(table -> assertThat(table.getIndexes().size()).isEqualTo(0));

        final var recordLayerTemplate = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);
        final var joinedTables = recordLayerTemplate.getJoinedSyntheticTables();
        assertThat(joinedTables.size()).isEqualTo(1);
        final var joinedTable = joinedTables.stream().findFirst().orElseThrow();
        assertThat(joinedTable.getName()).isEqualTo("__joined_" + indexName);

        // What the DDL produced has to serialize into valid RecordMetaData. RecordMetaDataBuilder.build()
        // validates, and JoinedRecordTypeBuilder rejects a join naming a constituent it does not know, so this
        // is what shows the synthetic table and its index are legally constructed — not merely well-formed in
        // the relational model.
        final var recordMetaData = Assertions.assertDoesNotThrow(recordLayerTemplate::toRecordMetadata);
        Assertions.assertTrue(recordMetaData.getSyntheticRecordTypes().containsKey(joinedTable.getName()),
                () -> "joined synthetic table missing from serialized metadata, got "
                        + recordMetaData.getSyntheticRecordTypes().keySet());
        return joinedTable;
    }

    /** The alias the n-th joined table is registered under, derived from the definition rather than from the plan. */
    @Nonnull
    private static String constituentAlias(final int constituentIndex) {
        return "joined_" + constituentIndex;
    }

    /** The key expression a constituent's column contributes: {@code field(alias).nest(column)}, no fan-out. */
    @Nonnull
    private static KeyExpression constituentColumn(@Nonnull final RecordLayerJoinedSyntheticTable joinedTable,
                                                   final int constituentIndex, @Nonnull final String column) {
        // Read back as well as named, so a drift between the two shows up here rather than in the key expression only.
        assertThat(joinedTable.getConstituents().get(constituentIndex).alias())
                .isEqualTo(constituentAlias(constituentIndex));
        return Key.Expressions.field(constituentAlias(constituentIndex), KeyExpression.FanType.None).nest(column);
    }

    @Nonnull
    private AbstractMetadataOperationsFactory assertingFactory(@Nonnull final java.util.function.Consumer<SchemaTemplate> assertions) {
        return new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                      @Nonnull Options templateProperties) {
                assertions.accept(template);
                return txn -> {
                };
            }
        };
    }

    // ─── A two-table join ─────────────────────────────────────────────────────────────────────

    private static void assertTwoTableJoin(@Nonnull final SchemaTemplate template,
                                           @Nonnull final String indexName) {
        final var joinedTable = soleJoinedTable(template, indexName);

        // One constituent per joined table, in FROM order.
        final var constituents = joinedTable.getConstituents();
        assertThat(constituents.size()).isEqualTo(2);
        assertThat(constituents.stream().map(RecordLayerJoinedSyntheticTable.JoinedConstituent::tableName)
                .collect(Collectors.toList())).isEqualTo(List.of("T1", "T2"));
        // Deterministic aliases: the same DDL always names its constituents the same way.
        assertThat(constituents.stream().map(RecordLayerJoinedSyntheticTable.JoinedConstituent::alias)
                .collect(Collectors.toList())).isEqualTo(List.of("joined_0", "joined_1"));

        // The WHERE/ON equality became the join condition, one side per constituent.
        assertThat(joinedTable.getJoinConditions().size()).isEqualTo(1);
        final var joinCondition = joinedTable.getJoinConditions().get(0);
        assertThat(joinCondition.leftAlias()).isEqualTo(constituents.get(0).alias());
        assertThat(joinCondition.rightAlias()).isEqualTo(constituents.get(1).alias());
        assertThat(joinCondition.leftExpression()).isEqualTo(Key.Expressions.field("k"));
        assertThat(joinCondition.rightExpression()).isEqualTo(Key.Expressions.field("k"));

        assertThat(joinedTable.getIndexes().size()).isEqualTo(1);
        final var index = joinedTable.getIndexes().stream().findFirst().orElseThrow();
        assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
        assertThat(index.getName()).isEqualTo(indexName);
        assertThat(index.getTableName()).isEqualTo(joinedTable.getName());
        // No index predicate: the equality is the join, not a filter.
        assertThat(index.getPredicate()).isNull();
        assertThat(index.getKeyExpression()).isEqualTo(Key.Expressions.concat(
                constituentColumn(joinedTable, 0, "x"),
                constituentColumn(joinedTable, 1, "y")));

        // Writes to either joined table maintain the index, so it is attributed to both.
        final var tableIndexMapping = Assertions.assertDoesNotThrow(template::getTableIndexMapping);
        Assertions.assertTrue(tableIndexMapping.get("T1").contains(indexName),
                () -> "expected index on T1, got " + tableIndexMapping);
        Assertions.assertTrue(tableIndexMapping.get("T2").contains(indexName),
                () -> "expected index on T2, got " + tableIndexMapping);
    }

    @Nonnull
    private static Stream<Arguments> twoTableSpellings() {
        return Stream.of(
                Arguments.of(COMMA_JOIN, "mv1",
                        "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                                + "order by a.x, b.y "),
                Arguments.of(INNER_JOIN, "mv1",
                        "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a INNER JOIN T2 AS b ON a.k = b.k "
                                + "order by a.x, b.y "),
                Arguments.of(USING_JOIN, "mv1",
                        "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a INNER JOIN T2 AS b USING (k) "
                                + "order by a.x, b.y "),
                Arguments.of(VIEW_COMMA_JOIN, "i1",
                        "CREATE VIEW mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                                + "CREATE INDEX i1 on mv1(x, y)"),
                Arguments.of(VIEW_INNER_JOIN, "i1",
                        "CREATE VIEW mv1 AS SELECT a.x, b.y FROM T1 AS a INNER JOIN T2 AS b ON a.k = b.k "
                                + "CREATE INDEX i1 on mv1(x, y)"),
                Arguments.of(VIEW_USING_JOIN, "i1",
                        "CREATE VIEW mv1 AS SELECT a.x, b.y FROM T1 AS a INNER JOIN T2 AS b USING (k) "
                                + "CREATE INDEX i1 on mv1(x, y)"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("twoTableSpellings")
    void createIndexOnTwoTableJoin(@Nonnull final String spelling, @Nonnull final String indexName,
                                   @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl,
                assertingFactory(template -> assertTwoTableJoin(template, indexName)));
    }

    // ─── Columns of one constituent at non-adjacent key positions ─────────────────────────────

    /**
     * The case a fan-out cannot express: two columns of the same table separated by a column of another. A
     * constituent is navigated with no fan-out, so it may be referenced at any number of key positions.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonAdjacentSpellings")
    void createIndexWithNonAdjacentColumnsOfOneTable(@Nonnull final String spelling,
                                                     @Nonnull final String indexDdl) throws Exception {
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var joinedTable = soleJoinedTable(template, "mv1");
            final var index = joinedTable.getIndexes().stream().findFirst().orElseThrow();
            assertThat(index.getKeyExpression()).isEqualTo(Key.Expressions.concat(
                    constituentColumn(joinedTable, 0, "x"),
                    constituentColumn(joinedTable, 1, "y"),
                    constituentColumn(joinedTable, 0, "k")));
        }));
    }

    @Nonnull
    private static Stream<Arguments> nonAdjacentSpellings() {
        return Stream.of(
                Arguments.of(COMMA_JOIN,
                        "CREATE INDEX mv1 AS SELECT a.x, b.y, a.k FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                                + "order by a.x, b.y, a.k "),
                Arguments.of(INNER_JOIN,
                        "CREATE INDEX mv1 AS SELECT a.x, b.y, a.k FROM T1 AS a INNER JOIN T2 AS b ON a.k = b.k "
                                + "order by a.x, b.y, a.k "));
    }

    // ─── Three tables, and a self-join ───────────────────────────────────────────────────────

    @Test
    void createIndexOnThreeTableJoin() throws Exception {
        final String indexDdl = "CREATE INDEX mv1 AS SELECT a.x, b.y, c.z FROM T1 AS a, T2 AS b, T3 AS c "
                + "WHERE a.k = b.k AND b.k = c.k order by a.x, b.y, c.z ";
        shouldWorkWithInjectedFactory(THREE_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var joinedTable = soleJoinedTable(template, "mv1");
            assertThat(joinedTable.getConstituents().stream()
                    .map(RecordLayerJoinedSyntheticTable.JoinedConstituent::tableName)
                    .collect(Collectors.toList())).isEqualTo(List.of("T1", "T2", "T3"));
            // Two equalities, each relating a different pair of constituents.
            assertThat(joinedTable.getJoinConditions().size()).isEqualTo(2);
            final var index = joinedTable.getIndexes().stream().findFirst().orElseThrow();
            assertThat(index.getKeyExpression()).isEqualTo(Key.Expressions.concat(
                    constituentColumn(joinedTable, 0, "x"),
                    constituentColumn(joinedTable, 1, "y"),
                    constituentColumn(joinedTable, 2, "z")));
        }));
    }

    /** A self-join registers the same table twice, under two different constituent aliases. */
    @Test
    void createIndexOnSelfJoin() throws Exception {
        final String indexDdl = "CREATE INDEX mv1 AS SELECT a.x, c.x FROM T1 AS a, T1 AS c WHERE a.k = c.x "
                + "order by a.x, c.x ";
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var joinedTable = soleJoinedTable(template, "mv1");
            final var constituents = joinedTable.getConstituents();
            assertThat(constituents.stream()
                    .map(RecordLayerJoinedSyntheticTable.JoinedConstituent::tableName)
                    .collect(Collectors.toList())).isEqualTo(List.of("T1", "T1"));
            // Same table, distinct aliases — otherwise the two sides could not be told apart.
            assertThat(constituents.get(0).alias()).isNotEqualTo(constituents.get(1).alias());
            final var joinCondition = joinedTable.getJoinConditions().get(0);
            assertThat(joinCondition.leftExpression()).isEqualTo(Key.Expressions.field("k"));
            assertThat(joinCondition.rightExpression()).isEqualTo(Key.Expressions.field("x"));
        }));
    }

    // ─── Shapes that are rejected ────────────────────────────────────────────────────────────

    // ─── Carried through from the stored-table path ──────────────────────────────────────────

    /**
     * A key column not covered by the {@code ORDER BY} becomes the index's value rather than part of its key,
     * exactly as on a stored table — the split point is applied to the constituent-rooted key expression.
     */
    @Test
    void createIndexOnJoinWithSplitPoint() throws Exception {
        final String indexDdl = "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                + "order by a.x ";
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var joinedTable = soleJoinedTable(template, "mv1");
            final var index = joinedTable.getIndexes().stream().findFirst().orElseThrow();
            assertThat(index.getIndexType()).isEqualTo(IndexTypes.VALUE);
            assertThat(index.getKeyExpression()).isEqualTo(Key.Expressions.keyWithValue(
                    Key.Expressions.concat(
                            constituentColumn(joinedTable, 0, "x"),
                            constituentColumn(joinedTable, 1, "y")),
                    1));
        }));
    }

    /**
     * A version column cannot be part of an index over a join. The row version belongs to a stored record, and a joined
     * record has none of its own, so taking one constituent's would make the index depend on which side the version was
     * read through — the same reason an unnested synthetic table rejects one.
     */
    @Test
    void versionColumnOnJoinIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.\"__ROW_VERSION\" FROM T1 AS a, T2 AS b "
                        + "WHERE a.k = b.k order by a.x, b.\"__ROW_VERSION\" WITH OPTIONS(store_row_versions=true)",
                "a version column cannot be part of an index over a joined synthetic table");
    }

    /**
     * An unnested and a joined synthetic table in one schema template stay independent: each gets its own
     * synthetic table of its own kind, carrying its own index, and the template still serializes into valid
     * {@link com.apple.foundationdb.record.RecordMetaData}. Recognising one kind must not change how the other
     * is built.
     */
    @Test
    void unnestedAndJoinedSyntheticTablesCoexist() throws Exception {
        final String indexDdl =
                // Columns of one table at non-adjacent key positions, so this needs a joined synthetic table.
                "CREATE INDEX ju AS SELECT a.x, b.y, a.k FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                        + "order by a.x, b.y, a.k "
                // Columns of one unnesting at non-adjacent key positions, so this needs an unnested one.
                + "CREATE INDEX un AS SELECT M.p, a.x, M.q FROM T1 AS a, a.a AS M order by M.p, a.x, M.q ";
        shouldWorkWithInjectedFactory(ARRAY_SCHEMA + indexDdl, assertingFactory(template -> {
            final var recordLayerTemplate = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);

            final var joinedTables = recordLayerTemplate.getJoinedSyntheticTables();
            assertThat(joinedTables.size()).isEqualTo(1);
            final var joinedTable = joinedTables.stream().findFirst().orElseThrow();
            assertThat(joinedTable.getName()).isEqualTo("__joined_ju");
            assertThat(joinedTable.getConstituents().size()).isEqualTo(2);
            assertThat(joinedTable.getJoinConditions().size()).isEqualTo(1);

            final var unnestedTables = recordLayerTemplate.getUnnestedSyntheticTables();
            assertThat(unnestedTables.size()).isEqualTo(1);
            final var unnestedTable = unnestedTables.stream().findFirst().orElseThrow();
            assertThat(unnestedTable.getName()).isEqualTo("__unnested_T1_un");
            assertThat(unnestedTable.getParentTableStorageName()).isEqualTo("T1");
            assertThat(unnestedTable.getConstituents().size()).isEqualTo(1);

            // Both kinds together still produce metadata the record layer accepts.
            final var recordMetaData = Assertions.assertDoesNotThrow(recordLayerTemplate::toRecordMetadata);
            Assertions.assertEquals(Set.of("__joined_ju", "__unnested_T1_un"),
                    recordMetaData.getSyntheticRecordTypes().keySet());

            // The joined index is attributed to both its tables, the unnested one only to its parent.
            final var tableIndexMapping = Assertions.assertDoesNotThrow(template::getTableIndexMapping);
            Assertions.assertTrue(tableIndexMapping.get("T1").containsAll(Set.of("ju", "un")),
                    () -> "expected both indexes on T1, got " + tableIndexMapping);
            Assertions.assertEquals(Set.of("ju"), Set.copyOf(tableIndexMapping.get("T2")),
                    () -> "expected only the joined index on T2, got " + tableIndexMapping);
        }));
    }

    /**
     * Asserts the definition is rejected. The transaction is managed here rather than going through
     * {@link DdlTestUtil#shouldWorkWithInjectedFactory}, which only rolls back once planning has succeeded —
     * leaving an open transaction behind on the failure path and breaking whatever test runs next.
     */
    private void shouldFailWith(@Nonnull final String query,
                                @Nonnull final String expectedMessageFragment) throws Exception {
        DdlTestUtil.shouldFailWith(connection, database.getSchemaTemplateName(), "/JoinedSyntheticTableIndexTest",
                query, ErrorCode.UNSUPPORTED_OPERATION, expectedMessageFragment);
    }

    @Test
    void outerJoinIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a LEFT OUTER JOIN T2 AS b ON a.k = b.k "
                        + "order by a.x, b.y ",
                "an outer join is not supported");
    }

    /**
     * The synthetic table's name is composed from the index name, which is a user identifier and so need not be a legal
     * protobuf message name. The relational model keeps the name as written, while the descriptor can only hold the
     * escaped form — so the record type has to be registered under the storage name, not the user-facing one.
     */
    @Test
    void createIndexWithNonProtoCompliantNameOverJoinedSyntheticTable() throws Exception {
        final String indexDdl = "CREATE INDEX \"mv.1\" AS SELECT a.x, b.y FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                + "order by a.x, b.y ";
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var recordLayerTemplate = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);
            final var joinedTable = recordLayerTemplate.getJoinedSyntheticTables().stream().findFirst().orElseThrow();
            // The relational model keeps the name as the definition wrote it.
            Assertions.assertEquals("__joined_mv.1", joinedTable.getName());
            // The descriptor, and so the metadata, is keyed by the escaped form.
            final var recordMetaData = Assertions.assertDoesNotThrow(recordLayerTemplate::toRecordMetadata);
            Assertions.assertTrue(recordMetaData.getSyntheticRecordTypes().containsKey("__joined_mv__21"),
                    () -> "expected the escaped name, got " + recordMetaData.getSyntheticRecordTypes().keySet());
        }));
    }

    /**
     * A filter on top of the join is stored with the index and evaluated against the joined record, so only the joined
     * records the definition asks for are indexed. The equality relating the tables still becomes a join condition; the
     * rest stays a predicate.
     */
    @Test
    void createIndexOnJoinWithResidualPredicate() throws Exception {
        final String indexDdl = "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b "
                + "WHERE a.k = b.k AND a.x > 5 order by a.x, b.y ";
        shouldWorkWithInjectedFactory(TWO_TABLE_SCHEMA + indexDdl, assertingFactory(template -> {
            final var joinedTable = soleJoinedTable(template, "mv1");
            // The join condition is still separated out from the filter.
            assertThat(joinedTable.getJoinConditions().size()).isEqualTo(1);
            final var index = joinedTable.getIndexes().stream().findFirst().orElseThrow();
            // The filter is stored with the index, rooted at the constituent it reads from.
            Assertions.assertNotNull(index.getPredicate(),
                    () -> "expected the residual filter to be stored as the index predicate");
            assertThat(index.getKeyExpression()).isEqualTo(Key.Expressions.concat(
                    constituentColumn(joinedTable, 0, "x"),
                    constituentColumn(joinedTable, 1, "y")));
        }));
    }

    @Test
    void crossTableFilterIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b "
                        + "WHERE a.k = b.k AND a.x > b.y order by a.x, b.y ",
                "cannot compare two of the joined tables");
    }

    @Test
    void nonEqualityJoinConditionIsRejected() throws Exception {
        // Only an equality can relate two constituents, so an inequality leaves the definition with no join condition
        // at all — which is what it is told, rather than being told the inequality is the problem.
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b WHERE a.k > b.k "
                        + "order by a.x, b.y ",
                "requires a join condition relating them");
    }

    @Test
    void crossJoinWithoutAConditionIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.y FROM T1 AS a, T2 AS b order by a.x, b.y ",
                "requires a join condition relating them");
    }

    @Test
    void joinOfSubqueriesIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT X.x, Y.y FROM (SELECT x FROM T1) X, (SELECT y FROM T2) Y "
                        + "order by X.x, Y.y ",
                "must be selected from directly rather than through a subquery");
    }

    @Test
    void aggregateOverJoinIsRejected() throws Exception {
        shouldFailWith(TWO_TABLE_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, count(b.y) FROM T1 AS a, T2 AS b WHERE a.k = b.k "
                        + "GROUP BY a.x ",
                "an aggregate cannot be defined on a joined synthetic table");
    }

    /** Unnesting and joining do not yet compose; a definition needing both is rejected rather than losing one. */
    @Test
    void unnestingCombinedWithJoinIsRejected() throws Exception {
        shouldFailWith(ARRAY_SCHEMA
                        + "CREATE INDEX mv1 AS SELECT a.x, b.y, M.p FROM T1 AS a, T2 AS b, a.a AS M "
                        + "WHERE a.k = b.k order by a.x, b.y, M.p ",
                "an unnesting cannot be combined with a join");
    }
}
