/*
 * UnnestedSyntheticTableIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.relational.util.NullableArrayUtils.REPEATED_FIELD_NAME;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;

/**
 * Indexes over an unnesting that need an unnested synthetic table, and the shapes that are rejected. A synthetic table
 * is needed exactly when two or more columns read through the same unnesting are non-adjacent in the index key; the
 * shapes that stay a fan-out on the stored table are covered by {@link IndexTest}.
 */
public class UnnestedSyntheticTableIndexTest {
    /**
     * The plan generator loads a store for the connection's schema to read its metadata, so a schema has to exist before
     * any of these statements can be planned. Every test then declares its own schema template, so nothing here reads
     * this table -- it is the smallest one that makes the connection usable.
     */
    private static final String PLACEHOLDER_SCHEMA = "CREATE TABLE placeholder(id bigint, primary key(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database =
            new SimpleDatabaseRule(UnnestedSyntheticTableIndexTest.class, PLACEHOLDER_SCHEMA);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    /**
     * Asserts that the definition is rejected, with a message containing {@code errorMessage}. Every shape this file
     * rejects is an unsupported one, so the code is always {@link ErrorCode#UNSUPPORTED_OPERATION}.
     *
     * @param query the DDL statement expected to be rejected
     * @param errorMessage a substring the rejection message has to contain
     * @throws Exception if anything other than planning fails
     */
    void shouldFailWith(@Nonnull final String query, @Nonnull final String errorMessage) throws Exception {
        DdlTestUtil.shouldFailWith(connection, database.getSchemaTemplateName(), "/UnnestedSyntheticTableIndexTest", query,
                ErrorCode.UNSUPPORTED_OPERATION, errorMessage);
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull final MetadataOperationsFactory metadataOperationsFactory)
            throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        Assertions.assertDoesNotThrow(() ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/UnnestedSyntheticTableIndexTest", metadataOperationsFactory)
                        .getPlan(query));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    /**
     * Asserts that the statement defines its index on an unnested synthetic table with a single nested
     * constituent, and that the index key matches.
     *
     * @param stmt the DDL statement
     * @param indexType the expected index type
     * @param expectedKey builds the expected key from the (parent alias, constituent alias)
     * @throws Exception if planning fails
     */
    private void syntheticIndexIs(@Nonnull final String stmt, @Nonnull final String indexType,
                                  @Nonnull final BiFunction<String, String, KeyExpression> expectedKey) throws Exception {
        syntheticIndexIs(stmt, indexType, 1, (parent, constituents) -> expectedKey.apply(parent, constituents.get(0)));
    }

    /**
     * Asserts that the statement defines its index on an unnested synthetic table, and that the index key
     * matches. The parent and constituent aliases are generated, so the expected key is built from the
     * aliases found in the metadata; constituents are given in registration order, outermost first.
     *
     * @param stmt the DDL statement
     * @param indexType the expected index type
     * @param constituentCount the expected number of nested constituents
     * @param expectedKey builds the expected key from the (parent alias, constituent aliases)
     * @throws Exception if planning fails
     */
    private void syntheticIndexIs(@Nonnull final String stmt, @Nonnull final String indexType, final int constituentCount,
                                  @Nonnull final BiFunction<String, List<String>, KeyExpression> expectedKey) throws Exception {
        syntheticIndexIs(stmt, indexType, constituentCount, expectedKey, (syntheticTable, metaData) -> { });
    }

    /**
     * As {@link #syntheticIndexIs(String, String, int, BiFunction)}, with an extra validator for assertions
     * that go beyond the index key, such as the constituent tree or the synthetic primary key.
     *
     * @param stmt the DDL statement
     * @param indexType the expected index type
     * @param constituentCount the expected number of nested constituents
     * @param expectedKey builds the expected key from the (parent alias, constituent aliases)
     * @param validator further assertions on the synthetic table and the metadata it serializes to
     * @throws Exception if planning fails
     */
    private void syntheticIndexIs(@Nonnull final String stmt, @Nonnull final String indexType, final int constituentCount,
                                  @Nonnull final BiFunction<String, List<String>, KeyExpression> expectedKey,
                                  @Nonnull final BiConsumer<RecordLayerUnnestedSyntheticTable, RecordMetaData> validator) throws Exception {
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options templateProperties) {
                final var syntheticTables = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                        .getUnnestedSyntheticTables();
                Assertions.assertEquals(1, syntheticTables.size(), "Incorrect number of synthetic tables!");
                final var syntheticTable = syntheticTables.stream().findFirst().orElseThrow();
                Assertions.assertEquals(constituentCount, syntheticTable.getConstituents().size(),
                        "Incorrect number of nested constituents!");
                final var constituentAliases = syntheticTable.getConstituents().stream()
                        .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getAlias)
                        .collect(Collectors.toList());
                syntheticTable.getConstituents().forEach(constituent ->
                        Assertions.assertTrue(constituent.getParentAlias().equals(syntheticTable.getAlias())
                                        || constituentAliases.contains(constituent.getParentAlias()),
                                () -> "constituent '" + constituent.getAlias() + "' has unknown parent '"
                                        + constituent.getParentAlias() + "'"));
                Assertions.assertEquals(1, syntheticTable.getIndexes().size(), "Incorrect number of indexes!");
                final var index = syntheticTable.getIndexes().stream().findFirst().orElseThrow();
                Assertions.assertEquals(indexType, index.getIndexType());
                Assertions.assertEquals(expectedKey.apply(syntheticTable.getAlias(), constituentAliases),
                        KeyExpression.fromProto(index.getKeyExpression().toKeyExpression()));
                final var metaData = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class).toRecordMetadata();
                // the descriptor is keyed by the storage name, which is the protobuf-compliant form of the declared one
                Assertions.assertTrue(metaData.getSyntheticRecordTypes().containsKey(syntheticTable.getType().getStorageName()),
                        () -> "synthetic type '" + syntheticTable.getType().getStorageName() + "' missing from serialized metadata, got "
                                + metaData.getSyntheticRecordTypes().keySet());
                validator.accept(syntheticTable, metaData);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createIndexWithRepeatedNestedSplitByField() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> keyWithValue(concat(
                field(x).nest("COL2"),
                field(parent).nest("COL5"),
                field(x).nest("COL3"),
                field(x).nest("COL4")), 3));
    }

    /**
     * As {@link #createIndexWithRepeatedNestedSplitByField()}, but over a table with a composite primary key,
     * one of whose columns is also an index key column.
     */
    @Test
    void createIndexWithRepeatedNestedSplitByFieldOverCompositePrimaryKey() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1, col5)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 1, (parent, constituents) -> keyWithValue(concat(
                        field(constituents.get(0)).nest("COL2"),
                        field(parent).nest("COL5"),
                        field(constituents.get(0)).nest("COL3"),
                        field(constituents.get(0)).nest("COL4")), 3),
                (syntheticTable, metaData) -> {
                    final var unnestedType = (UnnestedRecordType) metaData.getSyntheticRecordTypes()
                            .get(syntheticTable.getName());
                    final String constituent = syntheticTable.getConstituents().get(0).getAlias();
                    Assertions.assertEquals(
                            concat(Key.Expressions.recordType(), Key.Expressions.list(List.of(
                                    field(syntheticTable.getAlias()).nest(concat(Key.Expressions.recordType(),
                                            field("COL1"), field("COL5"))),
                                    field(UnnestedRecordType.POSITIONS_FIELD).nest(constituent)))),
                            unnestedType.getPrimaryKey(),
                            "the parent's full composite primary key should reach the synthetic type");
                });
    }

    /**
     * As {@link #createIndexWithRepeatedNestedSplitByField()}, but over an {@code ARRAY NOT NULL} column.
     */
    @Test
    void createIndexWithNestedRepeatedSplitOverNonNullableRepeated() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array not null, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> keyWithValue(concat(
                field(x).nest("COL2"),
                field(parent).nest("COL5"),
                field(x).nest("COL3"),
                field(x).nest("COL4")), 3));
    }

    /**
     * The synthetic table's name is composed from the index name, which is a user identifier and so need not be a legal
     * protobuf message name -- and the name does become one, in the synthetic record type's descriptor.
     */
    @Test
    void createIndexWithNonProtoCompliantNameOverUnnestedSyntheticTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX \"mv.1\" AS SELECT X.col2, T1.col5, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X " +
                "ORDER BY X.col2, T1.col5, X.col3";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 1,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("COL2"),
                        field(parent).nest("COL5"),
                        field(constituents.get(0)).nest("COL3")),
                (syntheticTable, metaData) -> {
                    Assertions.assertEquals("__unnested_T1_mv.1", syntheticTable.getName());
                    Assertions.assertTrue(metaData.getSyntheticRecordTypes().containsKey("__unnested_T1_mv__21"));
                });
    }

    /**
     * The same split, ordered by an explicit direction. The ordering functions are keyed by identity on the order-by
     * columns, so rewriting those columns onto the synthetic table has to re-key the map onto the rewritten values --
     * a column whose key is stale simply loses its direction, which no other assertion here would notice.
     */
    @Test
    void createIndexWithRepeatedNestedSplitByFieldRetainsOrderingFunctions() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X " +
                "ORDER BY X.col2 DESC, T1.col5, X.col3 NULLS LAST";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> concat(
                function("order_desc_nulls_last", field(x).nest("COL2")),
                field(parent).nest("COL5"),
                function("order_asc_nulls_last", field(x).nest("COL3"))));
    }

    /**
     * A scalar repeated field cannot be a constituent, so every reference to it is emitted as its own fan-out. Two
     * references to <em>one</em> unnesting would then range over the repeated field independently, and their cross
     * product holds entries where the two differ, which no view row does. No representation exists, so it is rejected.
     */
    @Test
    void createIndexWithScalarRepeatedReferencedTwiceIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, s string array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, V.s AS v1, X.col3, V.s AS v2 FROM T1, (SELECT col2, col3 FROM T1.A) X, (SELECT s FROM T1.S) V ORDER BY X.col2, v1, X.col3, v2";
        shouldFailWith(stmt, "a scalar array cannot be referenced at more than one index key position");
    }

    /**
     * The single-reference cases the check above must not disturb: one scalar reference stays a fan-out, whether
     * the repeated field hangs off the stored record or off an unnested element.
     */
    @Test
    void createIndexWithScalarRepeatedReferencedOnceKeepsFanOut() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, s string array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, V.s AS v1, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X, (SELECT s FROM T1.S) V ORDER BY X.col2, v1, X.col3";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> concat(
                field(x).nest("COL2"),
                field(parent).nest(field("S").nest(field("values", KeyExpression.FanType.FanOut))),
                field(x).nest("COL3")));
    }

    /**
     * Two <em>separate</em> explodes over the same scalar repeated field are distinct unnestings, so their cross-product is
     * the intended meaning of the cross join and each is referenced once.
     */
    @Test
    void createIndexWithTwoIndependentScalarUnnestingsIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, s string array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, V.s AS v1, X.col3, W.s AS v2 FROM T1, (SELECT col2, col3 FROM T1.A) X, (SELECT s FROM T1.S) V, (SELECT s FROM T1.S) W ORDER BY X.col2, v1, X.col3, v2";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> concat(
                field(x).nest("COL2"),
                field(parent).nest(field("S").nest(field("values", KeyExpression.FanType.FanOut))),
                field(x).nest("COL3"),
                field(parent).nest(field("S").nest(field("values", KeyExpression.FanType.FanOut)))));
    }

    /**
     * Two indexes in one template each need their own synthetic type, alongside a plain index on the stored table.
     */
    @Test
    void createTwoIndexesEachRequiringSyntheticTableKeepsThemSeparate() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3 " +
                "CREATE INDEX mv2 AS SELECT Y.col3, T1.col5, Y.col4 FROM T1, (SELECT col3, col4 FROM T1.A) Y ORDER BY Y.col3, T1.col5, Y.col4 " +
                "CREATE INDEX i3 AS SELECT T1.col5, T1.col1 FROM T1 ORDER BY T1.col5, T1.col1";
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options templateProperties) {
                final var recLayer = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);

                final var syntheticNames = recLayer.getUnnestedSyntheticTables().stream()
                        .map(RecordLayerUnnestedSyntheticTable::getName)
                        .collect(Collectors.toSet());
                Assertions.assertEquals(Set.of("__unnested_T1_MV1", "__unnested_T1_MV2"), syntheticNames);

                // one index each, and the plain index stays on the stored table
                recLayer.getUnnestedSyntheticTables().forEach(synthetic ->
                        Assertions.assertEquals(1, synthetic.getIndexes().size(),
                                () -> "expected one index on " + synthetic.getName()));
                final var storedTableIndexes = Assertions.assertDoesNotThrow(() ->
                        Assert.optionalUnchecked(template.findTableByName("T1")).getIndexes().stream()
                                .map(com.apple.foundationdb.relational.api.metadata.Index::getName)
                                .collect(Collectors.toSet()));
                Assertions.assertEquals(Set.of("I3"), storedTableIndexes);

                // both reach RecordMetaData, as distinct types with distinct record type keys
                final var metaData = recLayer.toRecordMetadata();
                Assertions.assertTrue(metaData.getSyntheticRecordTypes().keySet()
                                .containsAll(Set.of("__unnested_T1_MV1", "__unnested_T1_MV2")),
                        () -> "got " + metaData.getSyntheticRecordTypes().keySet());
                final var recordTypeKeys = metaData.getSyntheticRecordTypes().values().stream()
                        .map(com.apple.foundationdb.record.metadata.SyntheticRecordType::getRecordTypeKey)
                        .collect(Collectors.toSet());
                Assertions.assertEquals(2, recordTypeKeys.size(),
                        () -> "the two synthetic types share a record type key: " + recordTypeKeys);
                return txn -> {
                };
            }
        });
    }

    /**
     * The scalar repeated field lives on the unnested element type, not on the stored record, so its fan-out is
     * rooted at the constituent that owns it rather than at the parent.
     */
    @Test
    void createIndexWithScalarRepeatedInsideNestedRepeatedRootsFanOutAtConstituent() throws Exception {
        // A parent column between the struct's own columns is what splits the outer unnesting: `tg` sits inside
        // the struct element, so it traverses that unnesting too and cannot split it.
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint, tags string array, y bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT M.x, t.col1, M.y, tg FROM T1 AS t, t.a AS M, M.tags AS tg ORDER BY M.x, t.col1, M.y, tg";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> concat(
                field(x).nest("X"),
                field(parent).nest("COL1"),
                field(x).nest("Y"),
                field(x).nest(field("TAGS").nest(field("values", KeyExpression.FanType.FanOut)))));
    }

    /**
     * Aggregate Index is not supported.
     */
    @Test
    void createAggregateIndexOverUnnestedSyntheticTableIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint, x2 bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p)) " +
                "CREATE INDEX mv1 AS SELECT M.x, t.p, SUM(M.x2) FROM T AS t, t.a AS M GROUP BY M.x, t.p " +
                "ORDER BY M.x, t.p";
        shouldFailWith(stmt, "cannot be defined on an unnested synthetic table");
    }

    /**
     * Predicates are not supported.
     */
    @Test
    void createIndexWithPredicateOverUnnestedSyntheticTableIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X " +
                "WHERE T1.col5 > 10 ORDER BY X.col2, T1.col5, X.col3";
        shouldFailWith(stmt, "a predicate is not supported on an index over an unnested synthetic table");
    }

    /**
     * Two columns of the same unnesting separated by a column of a <em>different</em> unnesting, rather
     * than by a parent column. Still no single fan-out covers X, so this needs a synthetic type.
     */
    @Test
    void createIndexWithRepeatedNestedSplitByOtherRepeated() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col3, Y.col2, X.col4 FROM T1, (SELECT col3, col4 FROM T1.A) X, " +
                "(SELECT col2 FROM T1.A) Y ORDER BY X.col3, Y.col2, X.col4";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 2, (parent, constituents) -> concat(
                field(constituents.get(0)).nest("COL3"),
                field(constituents.get(1)).nest("COL2"),
                field(constituents.get(0)).nest("COL4")));
    }

    /**
     * Constituents branch as well as chain: {@code b} and {@code d} both hang off the stored record, while
     * {@code c} hangs off {@code b}.
     */
    @Test
    void createIndexWithBranchingAndChainedUnnestingUsesSyntheticTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT Q(y bigint, y2 bigint) " +
                "CREATE TYPE AS STRUCT P(x bigint, x2 bigint, q Q array) " +
                "CREATE TYPE AS STRUCT R(z bigint, z2 bigint) " +
                "CREATE TABLE A(k bigint, p P array, r R array, primary key(k)) " +
                "CREATE INDEX mv1 AS SELECT b.x, a.k, c.y, d.z FROM A AS a, (select * from a.p) as b, " +
                "(select * from b.q) as c, (select * from a.r) as d ORDER BY b.x, a.k, c.y, d.z";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 3, (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(parent).nest("K"),
                        field(constituents.get(1)).nest("Y"),
                        field(constituents.get(2)).nest("Z")),
                (syntheticTable, metaData) -> {
                    // The generic helper only checks that each parent alias is known, which cannot tell this
                    // tree apart from a chain, so pin the actual parent of each constituent.
                    final var constituents = syntheticTable.getConstituents();
                    Assertions.assertEquals(
                            List.of(syntheticTable.getAlias(), constituents.get(0).getAlias(), syntheticTable.getAlias()),
                            constituents.stream()
                                    .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getParentAlias)
                                    .collect(Collectors.toList()),
                            "constituents should branch at the stored record, with only the second one chained");
                    Assertions.assertEquals(
                            List.of(List.of("P", REPEATED_FIELD_NAME), List.of("Q", REPEATED_FIELD_NAME),
                                    List.of("R", REPEATED_FIELD_NAME)),
                            constituents.stream()
                                    .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getFieldPath)
                                    .collect(Collectors.toList()));
                });
    }

    /**
     * ROW_VERSION is not supported.
     */
    @Test
    void createVersionIndexWithRepeatedNestedSplitByVersion() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.\"__ROW_VERSION\", X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.\"__ROW_VERSION\", X.col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        shouldFailWith(stmt, "a version column cannot be part of an index over an unnested synthetic table");
    }

    /**
     * Complex values (anything other than column references) is not supported.
     */
    @Test
    void createIndexOverUnnestedSyntheticTableWithArithmeticColumnIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, T1.col5 + 1 AS pp FROM T1, (SELECT col2, col3 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3, pp";
        shouldFailWith(stmt, "supports only plain column references");
    }

    // Spellings

    private static final String VIEW_SUBQUERY = "view + correlated subquery";
    private static final String VIEW_PARTIQL = "view + PartiQL path";
    private static final String AS_SELECT_SUBQUERY = "index as select + correlated subquery";
    private static final String AS_SELECT_PARTIQL = "index as select + PartiQL path";

    // Schema Templates

    private static final String SINGLE_NESTED_REPEATED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, y bigint) " +
            "CREATE TABLE T(p bigint, a A array, primary key(p)) ";

    private static final String MULTIPLE_NESTED_REPEATED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, x2 bigint) CREATE TYPE AS STRUCT B(y bigint, y2 bigint) " +
            "CREATE TYPE AS STRUCT C(z bigint, z2 bigint) " +
            "CREATE TABLE T(p bigint, a A array, b B array, c C array, primary key(p)) ";

    private static final String MIXED_REPEATED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT A(x bigint, y bigint) " +
            "CREATE TABLE T(p bigint, a A array, s string array, primary key(p)) ";

    private static final String DEEP_NESTED_REPEATED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT S1(a string, b string) " +
            "CREATE TYPE AS STRUCT S2(x S1 array, y S1) " +
            "CREATE TYPE AS STRUCT S3(alpha S2, beta S2) " +
            "CREATE TABLE T(id bigint, fizz S3, buzz bigint, primary key(id)) ";

    private static final String CHAINED_NESTED_REPEATED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT Q(y bigint, y2 bigint) " +
            "CREATE TYPE AS STRUCT P(x bigint, x2 bigint, q Q array) " +
            "CREATE TABLE A(k bigint, p P array, primary key(k)) ";

    // ─── A single nested repeated field ──────────────────────────────────────────────────────────────

    @Nonnull
    private static Stream<Arguments> singleNestedRepeatedSpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "I1",
                        "CREATE VIEW mv1 AS SELECT SQ.x, t.p, SQ.y from T AS t, (select M.x, M.y from t.a AS M) SQ "
                                + "CREATE INDEX i1 on mv1(x, p, y)"),
                Arguments.of(VIEW_PARTIQL, "I1",
                        "CREATE VIEW mv1 AS SELECT M.x, t.p, M.y from T AS t, t.a AS M "
                                + "CREATE INDEX i1 on mv1(x, p, y)"),
                Arguments.of(AS_SELECT_SUBQUERY, "MV1",
                        "CREATE INDEX mv1 AS SELECT SQ.x, t.p, SQ.y from T AS t, (select M.x, M.y from t.a AS M) SQ "
                                + "order by SQ.x, t.p, SQ.y "),
                Arguments.of(AS_SELECT_PARTIQL, "MV1",
                        "CREATE INDEX mv1 AS SELECT M.x, t.p, M.y from T AS t, t.a AS M order by M.x, t.p, M.y "));
    }

    /**
     * What every spelling of a shape has to agree on beyond the index key: the name derived for the synthetic table, the
     * stored table it is built over, one nesting expression per constituent in registration order, the alias each
     * constituent hangs off, and the index it carries. The key itself, the constituent count and serialization are
     * asserted by {@link #syntheticIndexIs(String, String, int, BiFunction, BiConsumer)}.
     *
     * @param syntheticTable the table the index was defined on
     * @param storedTableName the stored table the synthetic table is built over
     * @param indexName the name the definition gives the index
     * @param nestingExpressions the expected nesting expression of each constituent, in order
     * @param parentAliases the alias each constituent is expected to hang off, in the same order
     */
    private static void assertUnnestedTableIs(@Nonnull final RecordLayerUnnestedSyntheticTable syntheticTable,
                                              @Nonnull final String storedTableName,
                                              @Nonnull final String indexName,
                                              @Nonnull final List<KeyExpression> nestingExpressions,
                                              @Nonnull final List<String> parentAliases) {
        Assertions.assertEquals("__unnested_" + storedTableName + "_" + indexName, syntheticTable.getName());
        Assertions.assertEquals(Set.of(storedTableName), syntheticTable.getUnderlyingTableNames());
        Assertions.assertEquals(nestingExpressions, syntheticTable.getConstituents().stream()
                .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getNestingExpression)
                .collect(Collectors.toList()));
        Assertions.assertEquals(parentAliases, syntheticTable.getConstituents().stream()
                .map(RecordLayerUnnestedSyntheticTable.NestedConstituent::getParentAlias)
                .collect(Collectors.toList()));
        final var index = syntheticTable.getIndexes().stream().findFirst().orElseThrow();
        Assertions.assertEquals(indexName, index.getName());
        Assertions.assertEquals(syntheticTable.getName(), index.getTableName());
    }

    /**
     * As {@link #assertUnnestedTableIs(RecordLayerUnnestedSyntheticTable, String, String, List, List)}, for a table over
     * {@code T} whose constituents all hang directly off the stored record.
     *
     * @param syntheticTable the table the index was defined on
     * @param indexName the name the definition gives the index
     * @param nestingExpressions the expected nesting expression of each constituent, in order
     */
    private static void assertUnnestedTableIs(@Nonnull final RecordLayerUnnestedSyntheticTable syntheticTable,
                                              @Nonnull final String indexName,
                                              @Nonnull final List<KeyExpression> nestingExpressions) {
        assertUnnestedTableIs(syntheticTable, "T", indexName, nestingExpressions,
                Collections.nCopies(nestingExpressions.size(), syntheticTable.getAlias()));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("singleNestedRepeatedSpellings")
    void createIndexOnNestedRepeatedSplitUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                               @Nonnull final String indexDdl) throws Exception {
        // constituent-alias paths with no fan-out: the fan-out lives in the constituent's nesting expression, and the
        // ORDER BY column order is preserved
        syntheticIndexIs(SINGLE_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 1,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(parent).nest("P"),
                        field(constituents.get(0)).nest("Y")),
                (syntheticTable, metaData) ->
                        assertUnnestedTableIs(syntheticTable, indexName, List.of(wrappedRepeatedElements("A"))));
    }

    // ─── Multiple nested repeated fields ─────────────────────────────────────────────────────────────

    /** Navigates to the elements of a nullable array, which is how the DDL layer stores {@code <T> array}. */
    @Nonnull
    private static KeyExpression wrappedRepeatedElements(@Nonnull final String arrayFieldName) {
        return field(arrayFieldName)
                .nest(field("values", KeyExpression.FanType.FanOut));
    }

    @Nonnull
    private static Stream<Arguments> multipleNestedRepeatedSpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "I1",
                        "CREATE VIEW v1 AS SELECT SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2 from T AS t, "
                                + "(select M.x, M.x2 from t.a AS M) SQ1, (select N.y, N.y2 from t.b AS N) SQ2, "
                                + "(select O.z, O.z2 from t.c AS O) SQ3 "
                                + "CREATE INDEX i1 on v1(x, y, z, p, x2, y2, z2)"),
                Arguments.of(VIEW_PARTIQL, "I1",
                        "CREATE VIEW v1 AS SELECT M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2 from T AS t, "
                                + "t.a AS M, t.b AS N, t.c AS O "
                                + "CREATE INDEX i1 on v1(x, y, z, p, x2, y2, z2)"),
                Arguments.of(AS_SELECT_SUBQUERY, "MV1",
                        "CREATE INDEX mv1 AS SELECT SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2 from T AS t, "
                                + "(select M.x, M.x2 from t.a AS M) SQ1, (select N.y, N.y2 from t.b AS N) SQ2, "
                                + "(select O.z, O.z2 from t.c AS O) SQ3 "
                                + "order by SQ1.x, SQ2.y, SQ3.z, t.p, SQ1.x2, SQ2.y2, SQ3.z2"),
                Arguments.of(AS_SELECT_PARTIQL, "MV1",
                        "CREATE INDEX mv1 AS SELECT M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2 from T AS t, "
                                + "t.a AS M, t.b AS N, t.c AS O "
                                + "order by M.x, N.y, O.z, t.p, M.x2, N.y2, O.z2"));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("multipleNestedRepeatedSpellings")
    void createIndexOnMultipleRepeatedUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                                       @Nonnull final String indexDdl) throws Exception {
        // one constituent per unnested repeated field, in declaration order, all parented to the stored record
        syntheticIndexIs(MULTIPLE_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 3,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(constituents.get(1)).nest("Y"),
                        field(constituents.get(2)).nest("Z"),
                        field(parent).nest("P"),
                        field(constituents.get(0)).nest("X2"),
                        field(constituents.get(1)).nest("Y2"),
                        field(constituents.get(2)).nest("Z2")),
                (syntheticTable, metaData) -> assertUnnestedTableIs(syntheticTable, indexName,
                        List.of(wrappedRepeatedElements("A"), wrappedRepeatedElements("B"), wrappedRepeatedElements("C"))));
    }

    @Nonnull
    private static Stream<Arguments> mixedRepeatedSpellings() {
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "I1",
                        "CREATE VIEW v1 AS SELECT SQ1.x, SQ2.v, SQ1.y from T AS t, "
                                + "(select M.x, M.y from t.a AS M) SQ1, (select v from t.s AS v) SQ2 "
                                + "CREATE INDEX i1 on v1(x, v, y)"),
                Arguments.of(VIEW_PARTIQL, "I1",
                        "CREATE VIEW v1 AS SELECT M.x, v, M.y from T AS t, t.a AS M, t.s AS v "
                                + "CREATE INDEX i1 on v1(x, v, y)"),
                Arguments.of(AS_SELECT_SUBQUERY, "MV1",
                        "CREATE INDEX mv1 AS SELECT SQ1.x, SQ2.v, SQ1.y from T AS t, "
                                + "(select M.x, M.y from t.a AS M) SQ1, (select v from t.s AS v) SQ2 "
                                + "order by SQ1.x, SQ2.v, SQ1.y"),
                Arguments.of(AS_SELECT_PARTIQL, "MV1",
                        "CREATE INDEX mv1 AS SELECT M.x, v, M.y from T AS t, t.a AS M, t.s AS v "
                                + "order by M.x, v, M.y"));
    }

    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("mixedRepeatedSpellings")
    void createIndexOnMixedRepeatedUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                                              @Nonnull final String indexDdl) throws Exception {
        // nested repeated element field via the constituent; scalar repeated element via a fan-out under the parent
        syntheticIndexIs(MIXED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 1,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(parent).nest(wrappedRepeatedElements("S")),
                        field(constituents.get(0)).nest("Y")),
                (syntheticTable, metaData) ->
                        assertUnnestedTableIs(syntheticTable, indexName, List.of(wrappedRepeatedElements("A"))));
    }

    @Nonnull
    private static Stream<Arguments> nestedPathSpellings() {
        // both repeated fields carry the same element type, so the view spellings have to alias the columns apart
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "I1",
                        "CREATE VIEW v1 AS SELECT u.a AS ua, v.a AS va, T.buzz, u.b AS ub, v.b AS vb "
                                + "FROM T, (SELECT a, b FROM T.fizz.alpha.x) AS u, (SELECT a, b FROM T.fizz.beta.x) AS v "
                                + "CREATE INDEX i1 on v1(ua, va, buzz, ub, vb)"),
                Arguments.of(VIEW_PARTIQL, "I1",
                        "CREATE VIEW v1 AS SELECT u.a AS ua, v.a AS va, T.buzz, u.b AS ub, v.b AS vb "
                                + "FROM T, T.fizz.alpha.x AS u, T.fizz.beta.x AS v "
                                + "CREATE INDEX i1 on v1(ua, va, buzz, ub, vb)"),
                Arguments.of(AS_SELECT_SUBQUERY, "MV1",
                        "CREATE INDEX mv1 AS SELECT u.a, v.a, T.buzz, u.b, v.b "
                                + "FROM T, (SELECT a, b FROM T.fizz.alpha.x) AS u, (SELECT a, b FROM T.fizz.beta.x) AS v "
                                + "ORDER BY u.a, v.a, T.buzz, u.b, v.b"),
                Arguments.of(AS_SELECT_PARTIQL, "MV1",
                        "CREATE INDEX mv1 AS SELECT u.a, v.a, T.buzz, u.b, v.b "
                                + "FROM T, T.fizz.alpha.x AS u, T.fizz.beta.x AS v "
                                + "ORDER BY u.a, v.a, T.buzz, u.b, v.b"));
    }

    /**
     * Two repeated fields, each reached through a path of non-repeated fields, split by a column of the stored record. The
     * constituent's nesting expression is relative to the record that owns the repeated field, so it has to carry every hop of
     * that path -- keeping only the repeated field would look for {@code X} directly on {@code T}, where it does not exist.
     */
    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("nestedPathSpellings")
    void createIndexOverNestedRepeatedUnderPathsUsesSyntheticTable(@Nonnull final String spelling, @Nonnull final String indexName,
                                                                  @Nonnull final String indexDdl) throws Exception {
        // distinct repeated fields under distinct paths, so distinct constituents
        syntheticIndexIs(DEEP_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 2,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("A"),
                        field(constituents.get(1)).nest("A"),
                        field(parent).nest("BUZZ"),
                        field(constituents.get(0)).nest("B"),
                        field(constituents.get(1)).nest("B")),
                (syntheticTable, metaData) -> assertUnnestedTableIs(syntheticTable, indexName,
                        List.of(repeatedElementsUnderPath("FIZZ", "ALPHA", "X"),
                                repeatedElementsUnderPath("FIZZ", "BETA", "X"))));
    }

    @Test
    void unnestedTableType() throws Exception {
        final String stmt = DEEP_NESTED_REPEATED_SCHEMA +
                "CREATE INDEX mv1 AS SELECT u.a, v.a, T.buzz, u.b, v.b " +
                "FROM T, (SELECT a, b FROM T.fizz.alpha.x) AS u, (SELECT a, b FROM T.fizz.beta.x) AS v " +
                "ORDER BY u.a, v.a, T.buzz, u.b, v.b";
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options templateProperties) {
                final var original = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);
                final var reloaded = RecordLayerSchemaTemplate.fromRecordMetadata(
                        original.toRecordMetadata(), original.getName(), original.getVersion());
                final var composed = Iterables.getOnlyElement(original.getUnnestedSyntheticTables());
                final var derived = Iterables.getOnlyElement(reloaded.getUnnestedSyntheticTables());
                Assertions.assertEquals(
                        List.of("parent", "unnesting_0", "unnesting_1", UnnestedRecordType.POSITIONS_FIELD),
                        fieldNamesOf(composed.getType()));
                Assertions.assertEquals(fieldNamesOf(composed.getType()), fieldNamesOf(derived.getType()));
                Assertions.assertEquals(composed.getType(), derived.getType(),
                        "the synthetic type composed from the index definition should equal the one derived from its descriptor");
                return txn -> {
                };
            }
        });
    }

    @Nonnull
    private static List<String> fieldNamesOf(@Nonnull final Type.Record type) {
        return type.getFields().stream()
                .map(Type.Record.Field::getFieldName)
                .collect(Collectors.toList());
    }

    @Nonnull
    private static Stream<Arguments> chainedSpellings(@Nonnull final String selectList, @Nonnull final String indexColumns) {
        final String subqueryForm = " FROM A AS a, (select * from a.p) as b, (select * from b.q) as c ";
        final String partiqlForm = " FROM A AS a, a.p AS b, b.q AS c ";
        return Stream.of(
                Arguments.of(VIEW_SUBQUERY, "I1", "CREATE VIEW v1 AS SELECT " + selectList + subqueryForm
                        + "CREATE INDEX i1 on v1(" + indexColumns + ")"),
                Arguments.of(VIEW_PARTIQL, "I1", "CREATE VIEW v1 AS SELECT " + selectList + partiqlForm
                        + "CREATE INDEX i1 on v1(" + indexColumns + ")"),
                Arguments.of(AS_SELECT_SUBQUERY, "MV1", "CREATE INDEX mv1 AS SELECT " + selectList + subqueryForm
                        + "ORDER BY " + selectList),
                Arguments.of(AS_SELECT_PARTIQL, "MV1", "CREATE INDEX mv1 AS SELECT " + selectList + partiqlForm
                        + "ORDER BY " + selectList));
    }

    /**
     * The constituent tree every chained spelling has to produce: {@code q} unnested under the element of {@code p}, so
     * the inner constituent hangs off the outer one rather than off the stored record, and its nesting expression is
     * relative to that element.
     *
     * @param syntheticTable the table the index was defined on
     * @param indexName the name the definition gives the index
     */
    private static void assertChainedTableIs(@Nonnull final RecordLayerUnnestedSyntheticTable syntheticTable,
                                             @Nonnull final String indexName) {
        assertUnnestedTableIs(syntheticTable, "A", indexName,
                List.of(wrappedRepeatedElements("P"), wrappedRepeatedElements("Q")),
                List.of(syntheticTable.getAlias(), syntheticTable.getConstituents().get(0).getAlias()));
    }

    @Nonnull
    private static Stream<Arguments> chainedSplitByParentSpellings() {
        return chainedSpellings("b.x, a.k, c.y", "x, k, y");
    }

    /**
     * Chained unnesting split by a parent column.
     */
    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("chainedSplitByParentSpellings")
    void createIndexWithChainedUnnestingSplitByParentUsesSyntheticTable(@Nonnull final String spelling,
                                                                        @Nonnull final String indexName,
                                                                        @Nonnull final String indexDdl) throws Exception {
        syntheticIndexIs(CHAINED_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 2,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(parent).nest("K"),
                        field(constituents.get(1)).nest("Y")),
                (syntheticTable, metaData) -> assertChainedTableIs(syntheticTable, indexName));
    }

    @Nonnull
    private static Stream<Arguments> chainedInnerSplitSpellings() {
        return chainedSpellings("c.y, a.k, c.y2", "y, k, y2");
    }

    /**
     * Chained unnesting where the split is within the inner unnesting.
     */
    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("chainedInnerSplitSpellings")
    void createIndexWithChainedUnnestingInnerSplitUsesSyntheticTable(@Nonnull final String spelling,
                                                                     @Nonnull final String indexName,
                                                                     @Nonnull final String indexDdl) throws Exception {
        syntheticIndexIs(CHAINED_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 2,
                (parent, constituents) -> concat(
                        field(constituents.get(1)).nest("Y"),
                        field(parent).nest("K"),
                        field(constituents.get(1)).nest("Y2")),
                (syntheticTable, metaData) -> assertChainedTableIs(syntheticTable, indexName));
    }

    /**
     * Chained unnesting where the split is within the outer unnesting.
     */
    @Nonnull
    private static Stream<Arguments> chainedOuterSplitSpellings() {
        return chainedSpellings("b.x, a.k, b.x2", "x, k, x2");
    }

    /**
     * The inner unnesting is never read from, yet it still multiplies the rows the index is built over, so it remains a
     * constituent.
     */
    @ParameterizedTest(name = "{displayName} - {0}")
    @MethodSource("chainedOuterSplitSpellings")
    void createIndexWithChainedUnnestingOuterSplitUsesSyntheticTable(@Nonnull final String spelling,
                                                                     @Nonnull final String indexName,
                                                                     @Nonnull final String indexDdl) throws Exception {
        syntheticIndexIs(CHAINED_NESTED_REPEATED_SCHEMA + indexDdl, IndexTypes.VALUE, 2,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("X"),
                        field(parent).nest("K"),
                        field(constituents.get(0)).nest("X2")),
                (syntheticTable, metaData) -> assertChainedTableIs(syntheticTable, indexName));
    }

    // ─── More nesting shapes: below the constituent, escaped names ───────────────────────────────────

    /**
     * A column reached through a non-repeated field <em>below</em> the constituent, so the re-rooted path is more than one
     * hop on the far side of the unnesting too.
     */
    @Test
    void createIndexOverNestedFieldBelowConstituentUsesSyntheticTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT S1(a string, b string) " +
                "CREATE TYPE AS STRUCT S2(part S1, tag string) " +
                "CREATE TABLE T(id bigint, items S2 array, buzz bigint, primary key(id)) " +
                "CREATE INDEX mv1 AS SELECT u.part.a, T.buzz, u.part.b " +
                "FROM T, (SELECT part FROM T.items) AS u ORDER BY u.part.a, T.buzz, u.part.b";
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, u) -> concat(
                field(u).nest(field("PART").nest("A")),
                field(parent).nest("BUZZ"),
                field(u).nest(field("PART").nest("B"))));
    }

    /**
     * A nested path whose intermediate field is not a legal protobuf identifier. Every hop of the nesting expression has
     * to be the storage name, so a declared name reaching the descriptor would not resolve.
     */
    @Test
    void createIndexOverNestedRepeatedUnderNonProtoCompliantPathUsesSyntheticTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT S1(a string, b string) " +
                "CREATE TYPE AS STRUCT S2(\"x.y\" S1 array) " +
                "CREATE TABLE T(id bigint, \"f.g\" S2, buzz bigint, primary key(id)) " +
                "CREATE INDEX mv1 AS SELECT u.a, T.buzz, u.b " +
                "FROM T, (SELECT a, b FROM T.\"f.g\".\"x.y\") AS u ORDER BY u.a, T.buzz, u.b";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 1,
                (parent, constituents) -> concat(
                        field(constituents.get(0)).nest("A"),
                        field(parent).nest("BUZZ"),
                        field(constituents.get(0)).nest("B")),
                (syntheticTable, metaData) -> Assertions.assertEquals(
                        repeatedElementsUnderPath("f__2g", "x__2y"),
                        syntheticTable.getConstituents().get(0).getNestingExpression()));
    }

    /**
     * Navigates to the elements of a nullable repeated field reached through the given path of non-repeated fields, by storage name.
     *
     * @param path the storage names to navigate, the repeated field last
     * @return the expected nesting expression
     */
    @Nonnull
    private static KeyExpression repeatedElementsUnderPath(@Nonnull final String... path) {
        var expression = wrappedRepeatedElements(path[path.length - 1]);
        for (int i = path.length - 2; i >= 0; i--) {
            expression = field(path[i]).nest(expression);
        }
        return expression;
    }
}
