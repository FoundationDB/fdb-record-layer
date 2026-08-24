/*
 * IndexTest.java
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

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexHelper;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionKeys;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.RecordMetaDataProto.AndPredicate;
import static com.apple.foundationdb.record.RecordMetaDataProto.Comparison;
import static com.apple.foundationdb.record.RecordMetaDataProto.ComparisonType;
import static com.apple.foundationdb.record.RecordMetaDataProto.Predicate;
import static com.apple.foundationdb.record.RecordMetaDataProto.RowNumberWindowPredicate;
import static com.apple.foundationdb.record.RecordMetaDataProto.SimpleComparison;
import static com.apple.foundationdb.record.RecordMetaDataProto.ValuePredicate;
import static com.apple.foundationdb.record.expressions.RecordKeyExpressionProto.Value;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.metadata.Key.Expressions.version;
import static com.apple.foundationdb.relational.util.NullableArrayUtils.REPEATED_FIELD_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class IndexTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(IndexTest.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @BeforeAll
    public static void setup() {
        Utils.enableCascadesDebugger();
    }

    void shouldFailWith(@Nonnull final String query, @Nonnull final ErrorCode errorCode, @Nonnull final String errorMessage) throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/IndexTest").getPlan(query));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        Assertions.assertTrue(ve.getMessage().contains(errorMessage), String.format(Locale.ROOT,
                "expected error message '%s' to contain '%s' but it didn't", ve.getMessage(), errorMessage));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull final MetadataOperationsFactory metadataOperationsFactory)
            throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        Assertions.assertDoesNotThrow(() ->
                DdlTestUtil.getPlanGenerator(connection.getUnderlyingEmbeddedConnection(), database.getSchemaTemplateName(),
                        "/IndexTest", metadataOperationsFactory)
                        .getPlan(query));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    private void indexIs(@Nonnull final String stmt, @Nonnull final KeyExpression expectedKey, @Nonnull final String indexType) throws Exception {
        indexIs(stmt, expectedKey, indexType, index -> { });
    }

    /**
     * Asserts that the statement defines its index on an unnested synthetic type with a single nested
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
     * Asserts that the statement defines its index on an unnested synthetic type, and that the index key
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
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options templateProperties) {
                final var syntheticTables = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class)
                        .getUnnestedSyntheticTables();
                Assertions.assertEquals(1, syntheticTables.size(), "Incorrect number of synthetic types!");
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
                Assertions.assertTrue(metaData.getSyntheticRecordTypes().containsKey(syntheticTable.getName()),
                        () -> "synthetic type '" + syntheticTable.getName() + "' missing from serialized metadata, got "
                                + metaData.getSyntheticRecordTypes().keySet());
                return txn -> {
                };
            }
        });
    }

    private void indexIs(@Nonnull final String stmt, @Nonnull final KeyExpression expectedKey, @Nonnull final String indexType,
                         @Nonnull final Consumer<RecordLayerIndex> validator) throws Exception {
        shouldWorkWithInjectedFactory(stmt, new AbstractMetadataOperationsFactory() {
            @Nonnull
            @Override
            public ConstantAction getSaveSchemaTemplateConstantAction(@Nonnull final SchemaTemplate template,
                                                                      @Nonnull final Options templateProperties) {
                Assertions.assertInstanceOf(RecordLayerSchemaTemplate.class, template);
                final var recordLayerSchemaTemplate = Assert.castUnchecked(template, RecordLayerSchemaTemplate.class);
                Assertions.assertEquals(1, recordLayerSchemaTemplate.getTables().size(), "Incorrect number of tables");
                final Table table = Assert.optionalUnchecked(recordLayerSchemaTemplate.getTables().stream().findFirst());
                Assertions.assertEquals(1, table.getIndexes().size(), "Incorrect number of indexes!");
                final Index index = Assert.optionalUnchecked(table.getIndexes().stream().findFirst());
                Assertions.assertInstanceOf(RecordLayerIndex.class, index);
                final var recordLayerIndex = (RecordLayerIndex)index;
                Assertions.assertEquals("MV1", index.getName(), "Incorrect index name!");
                Assertions.assertEquals(indexType, index.getIndexType());
                final KeyExpression actualKey = KeyExpression.fromProto((recordLayerIndex).getKeyExpression().toKeyExpression());
                Assertions.assertEquals(expectedKey, actualKey);
                validator.accept(recordLayerIndex);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createdIndexWorksSimpleNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint, y bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.F from T AS t, (select M.x as F from t.a AS M) SQ";
        indexIs(stmt, field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksSimpleNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.x, t.p from T AS t, (select M.x from t.a AS M) SQ order by SQ.x, t.p";
        indexIs(stmt, concat(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))), field("P")), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksSimpleNestingAndConcatDifferentOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT t.p, SQ.x from T AS t, (select M.x from t.a AS M) SQ ORDER BY t.p, SQ.x";
        indexIs(stmt, concat(field("P"), field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None)))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint, pp bigint) " +
                "CREATE TYPE AS STRUCT B(a A array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.x from T AS t, (select M.x from t.b AS Y, (select x, pp from Y.a) M) SQ";
        indexIs(stmt, field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                KeyExpression.FanType.FanOut).nest(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT C(z bigint) " +
                "CREATE TYPE AS STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ1.x,SQ2.z from " +
                "  T AS t," +
                "  (select M.x from t.b AS Y, (select x from Y.a) M) SQ1," +
                "  (select M.z from t.b AS Y, (select z from Y.c) M) SQ2" +
                " ORDER BY SQ1.x, SQ2.z";
        indexIs(stmt,
                concat(field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                                KeyExpression.FanType.FanOut).nest(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))))),
                        field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(),
                                KeyExpression.FanType.FanOut).nest(field("C", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("Z", KeyExpression.FanType.None)))))),
                IndexTypes.VALUE);
    }

    @Test
    void createdLegacyIndexWorksDeepNestingAndConcatCartesian() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT C(z bigint, k bigint) " +
                "CREATE TYPE AS STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ1.x,SQ2.z, SQ2.k from " +
                "  T AS t," +
                "  (select M.x from t.b AS Y, (select x from Y.a) M) SQ1," +
                "  (select M.z, M.k from t.b AS Y, (select z,k from Y.c) M) SQ2" +
                " ORDER BY SQ2.z, SQ2.k, SQ1.x";
        indexIs(stmt,
                concat(field("B").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                .nest(field("C").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                        .nest(concat(field("Z"), field("K")))))),
                        field("B").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                .nest(field("A").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                        .nest(field("X")))))),
                IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNestingAndConcatCartesian() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT C(z bigint, k bigint) " +
                "CREATE TYPE AS STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE VIEW v1 AS SELECT SQ1.x,SQ2.z, SQ2.k from " +
                "  T AS t," +
                "  (select M.x from t.b AS Y, (select x from Y.a) M) SQ1," +
                "  (select M.z, M.k from t.b AS Y, (select z,k from Y.c) M) SQ2 " +
                "CREATE INDEX MV1 ON v1(Z, K, X)";
        indexIs(stmt,
                concat(field("B").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                .nest(field("C").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                        .nest(concat(field("Z"), field("K")))))),
                        field("B").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                .nest(field("A").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)
                                        .nest(field("X")))))),
                IndexTypes.VALUE);
    }

    @Test
    void createdLegacyIndexWorksDeepNestingAndNestedCartesianConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT C(z bigint) " +
                "CREATE TYPE AS STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.x, SQ.z from T AS t, (select M.x, N.z from t.b AS Y, (select x from Y.a) M, (select z from Y.c) N) SQ ORDER BY SQ.x, SQ.z";
        indexIs(stmt,
                field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(
                        concat(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))),
                                field("C", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("Z", KeyExpression.FanType.None)))
                        ))),
                IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNestingAndNestedCartesianConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT C(z bigint) " +
                "CREATE TYPE AS STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p bigint, b B array, primary key(p))" +
                "CREATE VIEW v1 AS SELECT SQ.x, SQ.z from T AS t, (select M.x, N.z from t.b AS Y, (select x from Y.a) M, (select z from Y.c) N) SQ  " +
                "CREATE INDEX mv1 on v1(x, z)";
        indexIs(stmt,
                field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(
                        concat(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))),
                                field("C", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("Z", KeyExpression.FanType.None)))
                        ))),
                IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via correlated subquery: STRING ARRAY, INDEX…AS syntax.
     */
    @Test
    void createIndexOnScalarStringArray() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template "
                + "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.item FROM T AS t, (SELECT item FROM t.items AS item) SQ ORDER BY SQ.item";
        indexIs(stmt, field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via correlated subquery: STRING ARRAY, VIEW + INDEX…ON syntax.
     */
    @Test
    void createIndexOnScalarStringArrayUsingView() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE VIEW v1 AS SELECT SQ.item FROM T AS t, (SELECT item FROM t.items AS item) SQ " +
                "CREATE INDEX mv1 ON v1(item)";
        indexIs(stmt, field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via correlated subquery: INTEGER ARRAY (to exercise a different scalar type).
     */
    @Test
    void createIndexOnScalarIntegerArray() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, nums INTEGER ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.num FROM T AS t, (SELECT num FROM t.nums AS num) SQ ORDER BY SQ.num";
        indexIs(stmt, field("NUMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via correlated subquery: array element + table column, ordered by (item, p).
     */
    @Test
    void createIndexOnScalarArrayAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.item, t.p FROM T AS t, (SELECT item FROM t.items AS item) SQ ORDER BY SQ.item, t.p";
        indexIs(stmt, concat(field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), field("P")), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via correlated subquery: array element + table column, ordered by (p, item).
     */
    @Test
    void createIndexOnScalarArrayAndConcatDifferentOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT t.p, SQ.item FROM T AS t, (SELECT item FROM t.items AS item) SQ ORDER BY t.p, SQ.item";
        indexIs(stmt, concat(field("P"), field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut))), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via PartiQL syntax: STRING ARRAY, INDEX…AS syntax.
     */
    @Test
    void createIndexOnScalarStringArrayPartiQL() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT item FROM T AS t, t.items AS item ORDER BY item";
        indexIs(stmt, field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via PartiQL syntax: STRING ARRAY, VIEW + INDEX…ON syntax.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void createIndexOnScalarStringArrayPartiQLUsingView() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE VIEW v1 AS SELECT item FROM T AS t, t.items AS item " +
                "CREATE INDEX mv1 ON v1(item)";
        indexIs(stmt, field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via PartiQL syntax: INTEGER ARRAY (different scalar type).
     */
    @Test
    void createIndexOnScalarIntegerArrayPartiQL() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, nums INTEGER ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT num FROM T AS t, t.nums AS num ORDER BY num";
        indexIs(stmt, field("NUMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via PartiQL syntax: array element + table column, ordered by (item, p).
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void createIndexOnScalarArrayPartiQLAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT item, t.p FROM T AS t, t.items AS item ORDER BY item, t.p";
        indexIs(stmt, concat(field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut)), field("P")), IndexTypes.VALUE);
    }

    /**
     * Scalar array unnesting via PartiQL syntax: array element + table column, ordered by (p, item).
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void createIndexOnScalarArrayPartiQLAndConcatDifferentOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p BIGINT, items STRING ARRAY, PRIMARY KEY (p)) " +
                "CREATE INDEX mv1 AS SELECT t.p, item FROM T AS t, t.items AS item ORDER BY t.p, item";
        indexIs(stmt, concat(field("P"), field("ITEMS").nest(field(REPEATED_FIELD_NAME, KeyExpression.FanType.FanOut))), IndexTypes.VALUE);
    }

    @Test
    void createLegacyIndexWithPredicateIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TABLE T(p bigint, a A array, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT p FROM T where p > 10 order by p";
        indexIs(stmt, field("P", KeyExpression.FanType.None), IndexTypes.VALUE, index -> {
            assertThat(index.isUnique()).isFalse();
            assertThat(index.getName()).isEqualTo("MV1");
            assertThat(index.getPredicate()).isEqualTo(Predicate.newBuilder()
                    .setValuePredicate(ValuePredicate.newBuilder().addValue("P")
                            .setComparison(Comparison.newBuilder()
                                    .setSimpleComparison(SimpleComparison.newBuilder()
                                            .setType(ComparisonType.GREATER_THAN)
                                            .setOperand(Value.newBuilder().setLongValue(10L).build())
                                            .build())
                                    .build())
                            .build())
                    .build());
        });
    }

    @Test
    void createSlidingWindowValueIndexIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TYPE AS STRUCT C(z string) " +
                "CREATE TABLE T(p bigint, a A array, b B array, c C, primary key(p))" +
                "CREATE VIEW v AS SELECT p FROM T where p > 10 qualify row_number() over (order by c) <= 10 " +
                "CREATE INDEX mv1 ON v(p)";
        indexIs(stmt, field("P", KeyExpression.FanType.None), IndexTypes.VALUE, index -> {
            assertThat(index.isUnique()).isFalse();
            assertThat(index.getPredicate()).isEqualTo(Predicate.newBuilder()
                    .setAndPredicate(AndPredicate.newBuilder()
                            .addChildren(Predicate.newBuilder()
                                    .setValuePredicate(ValuePredicate.newBuilder().addValue("P")
                                            .setComparison(Comparison.newBuilder()
                                                    .setSimpleComparison(SimpleComparison.newBuilder()
                                                            .setType(ComparisonType.GREATER_THAN)
                                                            .setOperand(Value.newBuilder().setLongValue(10L).build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .addChildren(Predicate.newBuilder()
                                    .setRowNumberWindowPredicate(RowNumberWindowPredicate.newBuilder()
                                            .addOrderingField("C")
                                            .setSize(10)
                                            .setDirection(RowNumberWindowPredicate.Direction.ASC)
                                            .build())
                                    .build())
                            .build())
                    .build());
        });
    }

    @Test
    void createSlidingWindowValueIndexWithoutWhereClause() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, score bigint, primary key(p)) " +
                "CREATE VIEW v AS SELECT p FROM T qualify row_number() over (order by score) <= 50 " +
                "CREATE INDEX mv1 ON v(p)";
        indexIs(stmt, field("P", KeyExpression.FanType.None), IndexTypes.VALUE, index -> {
            assertThat(index.getPredicate()).isEqualTo(Predicate.newBuilder()
                    .setRowNumberWindowPredicate(RowNumberWindowPredicate.newBuilder()
                            .addOrderingField("SCORE")
                            .setSize(50)
                            .setDirection(RowNumberWindowPredicate.Direction.ASC)
                            .build())
                    .build());
        });
    }

    @Test
    void createIndexWithPredicateIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TABLE T(p bigint, a A array, b B array, primary key(p))" +
                "CREATE VIEW v AS SELECT p FROM T where p > 10 " +
                "CREATE INDEX mv1 ON v(p)";
        // todo (yhatem) verify the predicate.
        indexIs(stmt, field("P", KeyExpression.FanType.None), IndexTypes.VALUE, index -> {
            assertThat(index.isUnique()).isFalse();
            assertThat(index.getPredicate()).isEqualTo(Predicate.newBuilder()
                    .setValuePredicate(ValuePredicate.newBuilder().addValue("P")
                            .setComparison(Comparison.newBuilder()
                                    .setSimpleComparison(SimpleComparison.newBuilder()
                                            .setType(ComparisonType.GREATER_THAN)
                                            .setOperand(Value.newBuilder().setLongValue(10L).build())
                                            .build())
                                    .build())
                            .build())
                    .build());
        });
    }

    @Test
    void createIndexWithImproperNestedFieldClusteringIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TABLE T1(p1 bigint, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 bigint, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT X.p1,Y.p2 FROM (SELECT p1, a1,c1 FROM T1) X, (SELECT p2, b2 FROM T2) Y order by x.p1, y.p2";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported query, expected to find exactly one type filter operator");
    }

    @Test
    void createIndexWithJoiningMoreThanOneTableIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TABLE T1(p1 bigint, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 bigint, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT * FROM T1, T2 order by t1.p1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported query, expected to find exactly one type filter operator");
    }

    @Test
    void createIndexWithConstantArithmethicInProjectionIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TYPE AS STRUCT B(y string) " +
                "CREATE TABLE T1(p1 bigint, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT 5+1 FROM T1";
        indexIs(stmt, function("add", concat(value(5), value(1))), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithCardinalityFunctionOnNonNullableArrayIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 BIGINT, int_arr INTEGER ARRAY NOT NULL, PRIMARY KEY(p1)) " +
                "CREATE INDEX mv1 AS SELECT CARDINALITY(int_arr) FROM T1";
        var exp = function("cardinality", field("INT_ARR", KeyExpression.FanType.Concatenate));
        indexIs(stmt, exp, IndexTypes.VALUE);
    }

    @Test
    void createIndexWithCardinalityFunctionOnNullableArrayIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 BIGINT, int_arr INTEGER ARRAY NULL, PRIMARY KEY(p1)) " +
                "CREATE INDEX mv1 AS SELECT CARDINALITY(int_arr) FROM T1";
        // Notice the nested "values" field that gets introduced when the array is nullable.
        var exp = function("cardinality", field("INT_ARR").nest(field("values", KeyExpression.FanType.Concatenate)));
        indexIs(stmt, exp, IndexTypes.VALUE);
    }

    @Test
    void createIndexWithFieldSumInProjectionIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a + b FROM T1";
        indexIs(stmt, function("add", concat(field("A"), field("B"))), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithBitMaskInProjectionIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a & 4 FROM T1";
        indexIs(stmt, function("bitand", concat(field("A"), value(4))), IndexTypes.VALUE);
    }

    @Test
    void createBitMapIndexIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT bitmap_construct_agg(bitmap_bit_position(p1)) as bitmap, " +
                "a, b, bitmap_bucket_offset(p1) as offset FROM T1\n" +
                "GROUP BY a, b, bitmap_bucket_offset(p1)";
        indexIs(stmt, field("P1").groupBy(concat(field("A"), field("B"))), IndexTypes.BITMAP_VALUE);
    }

    @Test
    void createBitMapIndexWithEmptyGroupIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT bitmap_construct_agg(bitmap_bit_position(p1)) as bitmap, " +
                "bitmap_bucket_offset(p1) as offset FROM T1\n" +
                "GROUP BY bitmap_bucket_offset(p1)";
        indexIs(stmt, field("P1").ungrouped(), IndexTypes.BITMAP_VALUE);
    }

    @Test
    void createBitMapIndexWithRedundantFunctionsIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT bitmap_construct_agg(bitmap_bit_position(p1)) as bitmap, " +
                "a, bitmap_bucket_offset(p1), b, bitmap_bucket_offset(p1) as offset FROM T1\n" +
                "GROUP BY a, bitmap_bucket_offset(p1), b, bitmap_bucket_offset(p1)";
        shouldFailWith(stmt, ErrorCode.AMBIGUOUS_COLUMN, "Ambiguous columns for");
    }

    @Test
    void createIndexWithMultipleFunctionsInProjectionIsSupported() throws Exception {
        String functions = "a & 2, a | 4, a ^ 8, b + c, b - c, b * c, b / c, b % c";
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, c bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT " + functions + " FROM T1 ORDER BY " + functions;
        indexIs(stmt, concat(
                function("bitand", concat(field("A"), value(2))),
                function("bitor", concat(field("A"), value(4))),
                function("bitxor", concat(field("A"), value(8))),
                function("add", concat(field("B"), field("C"))),
                function("sub", concat(field("B"), field("C"))),
                function("mul", concat(field("B"), field("C"))),
                function("div", concat(field("B"), field("C"))),
                function("mod", concat(field("B"), field("C")))
        ), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithSomeFunctionsOnlyCoveringIsSupported() throws Exception {
        String functions = "a & 2, a | 2, a ^ 2, b + c, b - c, b * c, b / c, b % c";
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, c bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT " + functions + " FROM T1 ORDER BY a & 2, b - c";
        indexIs(stmt, new KeyWithValueExpression(concat(
                function("bitand", concat(field("A"), value(2))),
                function("sub", concat(field("B"), field("C"))),
                function("bitor", concat(field("A"), value(2))),
                function("bitxor", concat(field("A"), value(2))),
                function("add", concat(field("B"), field("C"))),
                function("mul", concat(field("B"), field("C"))),
                function("div", concat(field("B"), field("C"))),
                function("mod", concat(field("B"), field("C")))
        ), 2), IndexTypes.VALUE);
    }

    @Test
    void createAggregateIndexWithComplexGroupingExpressionCase1() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a bigint, b bigint, c bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a & 2, b + 3, MAX(b) FROM T1 GROUP BY a & 2, b + 3";
        indexIs(stmt, field("B").groupBy(concat(function("bitand", concat(field("A"), value(2))),
                function("add", concat(field("B"), value(3))))), IndexTypes.PERMUTED_MAX);
    }

    @Test
    void createSimpleValueIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1 FROM T1";
        indexIs(stmt,
                field("A1"),
                IndexTypes.VALUE
        );
    }

    @Test
    void createSimpleValueIndexOnTwoCols() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a1, a2";
        indexIs(stmt,
                concat(field("A1"), field("A2")),
                IndexTypes.VALUE);
    }

    @Test
    void createSimpleValueIndexOnNestedCol() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT S1(S1_1 bigint, S1_2 bigint) " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 S1, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a2.S1_1 FROM T1 order by a2.S1_1";
        indexIs(stmt, field("A2").nest(field("S1_1")),
                IndexTypes.VALUE);
    }

    @Test
    void createSimpleValueIndexOnTwoColsReverse() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a2, a1";
        indexIs(stmt,
                concat(field("A2"), field("A1")),
                IndexTypes.VALUE);
    }

    @Test
    void createCoveringValueIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, a3 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2, a3 FROM T1 order by a1, a2";
        indexIs(stmt,
                keyWithValue(concat(field("A1"), field("A2"), field("A3")), 2),
                IndexTypes.VALUE
        );
    }

    @Test
    void createIndexWithoutTopOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "indexes must have an order by clause at the top level");
    }

    @Test
    void createIndexOrderByUnknownColumns() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a4";
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN, "non existing column");
    }

    @Test
    void createIndexOrderByUnprojectedColumn() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, a2 bigint, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1 FROM T1 order by a2";
        shouldFailWith(stmt, ErrorCode.INVALID_COLUMN_REFERENCE, "not present in the projection list");
    }

    @Test
    void createIndexWithImproperNestedFieldClusteringInOrderByIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 bigint, a1 bigint, c1 string, primary key(p1)) " +
                "CREATE TABLE T2(p2 bigint, a2 bigint, b2 string, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT X.a1,X.c1, Y.b2 FROM (SELECT a1,c1 FROM T1) X, (SELECT b2 FROM T2) Y order by x.a1, y.b2, x.c1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported query, expected to find exactly one type filter operator");
    }

    @Test
    void createIndexWithNestedRepeatedSameParent() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT T1.col5, X.col3, X.col4 FROM T1, (SELECT col3, col4 FROM T1.A) X ORDER BY T1.col5, X.col3";
        indexIs(stmt, keyWithValue(concat(field("COL5"), field("A").nest(field("values", KeyExpression.FanType.FanOut).nest(concatenateFields("COL3", "COL4")))), 2), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithNestedRepeatedCartesianProduct() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT T1.col5, X.col3, Y.col4 FROM T1, (SELECT col3 FROM T1.A) X, (SELECT col4 FROM T1.A) Y ORDER BY T1.col5, X.col3";
        indexIs(stmt, keyWithValue(concat(field("COL5"), field("A").nest(field("values", KeyExpression.FanType.FanOut).nest("COL3")), field("A").nest(field("values", KeyExpression.FanType.FanOut).nest("COL4"))), 2), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithRepeatedNestedSplitByField() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3";
        // X.col2 is separated from X.col3/X.col4 by T1.col5, so no single fan-out can cover all three.
        // A fan-out key expression rejected this outright; an unnested synthetic type expresses it, since a
        // constituent holds one array element and can be referenced at any number of key positions.
        syntheticIndexIs(stmt, IndexTypes.VALUE, (parent, x) -> keyWithValue(concat(
                field(x).nest("COL2"),
                field(parent).nest("COL5"),
                field(x).nest("COL3"),
                field(x).nest("COL4")), 3));
    }

    /**
     * As {@link #createIndexWithRepeatedNestedSplitByField()}, but over an {@code ARRAY NOT NULL} column, which is
     * stored as a plain repeated field rather than wrapped in a {@code { repeated T values; }} message. The
     * constituent's nesting expression has to follow the declared storage form, so this only serializes if the
     * generator reads the declared field type rather than the expression's null-propagated one.
     */
    @Test
    void createIndexWithRepeatedNestedSplitByFieldOverNonNullableArray() throws Exception {
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
     * The unnesting is reached only through an enclosing arithmetic expression, so the key columns are not plain
     * {@code FieldValue}s. Two such columns are non-adjacent, which needs a synthetic type, but a synthetic
     * type's key can only be expressed in constituent-alias paths — so this is rejected rather than silently
     * falling back to a fan-out, which would cross-multiply the array against itself.
     */
    @Test
    void createIndexWithUnnestingReachedThroughExpressionIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col3 + 1 AS s1, T1.col5, X.col4 + 1 AS s2 FROM T1, (SELECT col3, col4 FROM T1.A) X ORDER BY s1, T1.col5, s2";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "supports only plain column references");
    }

    /**
     * As {@link #createIndexWithRepeatedNestedSplitByField()}, but with an extra arithmetic column. The shape
     * needs a synthetic type, and the arithmetic column cannot be rewritten into a constituent-alias path.
     */
    @Test
    void createIndexOverUnnestedSyntheticTypeWithArithmeticColumnIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3, T1.col5 + 1 AS pp FROM T1, (SELECT col2, col3 FROM T1.A) X ORDER BY X.col2, T1.col5, X.col3, pp";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "supports only plain column references");
    }

    /**
     * A scalar array cannot be a constituent, so each reference to it is emitted as its own fan-out. Two
     * references would range over the array independently, so the same view column could take different values
     * within one index entry. Rejected, matching what the stored-table path already does for this shape.
     */
    @Test
    void createIndexWithScalarArrayReferencedTwiceIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, s string array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, V.s AS v1, X.col3, V.s AS v2 FROM T1, (SELECT col2, col3 FROM T1.A) X, (SELECT s FROM T1.S) V ORDER BY X.col2, v1, X.col3, v2";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "a scalar array cannot be referenced at more than one index key position");
    }

    /**
     * The single-reference cases the check above must not disturb: one scalar reference stays a fan-out, whether
     * the array hangs off the stored record or off an unnested struct element.
     */
    @Test
    void createIndexWithScalarArrayReferencedOnceKeepsFanOut() throws Exception {
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
     * Two <em>separate</em> explodes over the same scalar array are distinct unnestings, so their cross-product is
     * the intended meaning of the cross join and each is referenced once. This must stay allowed even though the
     * emitted key looks identical to {@link #createIndexWithScalarArrayReferencedTwiceIsNotSupported()} — which is
     * why the check keys on unnesting identity rather than on the array's field path.
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
     * Exercises per-index naming of the generated types, accumulation through {@code addSyntheticTable}, and two
     * {@code UnnestedRecordType}s coexisting in one {@code RecordMetaData} with distinct record type keys.
     */
    @Test
    void createTwoIndexesEachRequiringSyntheticTypeKeepsThemSeparate() throws Exception {
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
     * The scalar array lives on the unnested struct's element type, not on the stored record, so its fan-out is
     * rooted at the constituent that owns it rather than at the parent. Every other mixed struct+scalar test puts
     * the scalar array on the table.
     */
    @Test
    void createIndexWithScalarArrayInsideUnnestedStructRootsFanOutAtConstituent() throws Exception {
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

    @Test
    void createIndexWithRepeatedNestedCartesianSplitByField() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT Y.col2, T1.col5, X.col3, X.col4 FROM T1, (SELECT col3, col4 FROM T1.A) X, (SELECT col2 FROM T1.A) Y ORDER BY Y.col2, T1.col5, X.col3";
        indexIs(stmt, keyWithValue(concat(field("A").nest(field("values", KeyExpression.FanType.FanOut).nest("COL2")), field("COL5"), field("A").nest(field("values", KeyExpression.FanType.FanOut).nest(concatenateFields("COL3", "COL4")))), 3), IndexTypes.VALUE);
    }

    @Test
    void createIndexWithNonRepeatedNestedSplitByField() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT T1.a.col2, T1.col5, T1.a.col3, T1.a.col4 FROM T1 ORDER BY T1.a.col2, T1.col5, T1.a.col3";
        // In theory, this should be fine, as the nested value is not repeated, but this is currently not distinguished by the index generator
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Index with multiple disconnected references to the same column are not supported");
    }

    @Test
    void createAggregateIndexWithGroupByContainingMoreThanOneAggregationIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2), COUNT(col2) FROM T1 GROUP BY col3, col4";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found group by expression with more than one aggregation");
    }

    /**
     * Repeating the same aggregate gets past the group-by validation, so it reaches the aggregate branch with more
     * than one aggregate value. Without the guard there the extra aggregation is silently dropped.
     */
    @Test
    void createAggregateIndexWithRepeatedAggregationIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT col1, SUM(col2), SUM(col2) FROM T1 GROUP BY col1 ORDER BY col1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by aggregations found");
    }

    @Test
    void createNestedAggregateIndexIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(h) FROM (SELECT sum(col2) as H FROM T1 GROUP BY col1) as x";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by expressions found");
    }

    @Test
    void multipleSelectsOverGroupBy() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT * FROM (SELECT * FROM (SELECT count(col2), sum(col2) from t1 group by col3, col4) B) A";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found group by expression with more than one aggregation");
    }

    @Test
    void createIndexAsSelectWithGroupByWorks() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2), col3, col4 FROM T1 GROUP BY col3, col4";
        indexIs(stmt,
                field("COL2").groupBy(field("COL3"), field("COL4")),
                IndexTypes.SUM
        );
    }

    @Test
    void createIndexAsSelectWithGroupByWithoutExplicitProjectionOfGroupingValuesWorks() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2) FROM T1 GROUP BY col3, col4";
        indexIs(stmt,
                field("COL2").groupBy(field("COL3"), field("COL4")),
                IndexTypes.SUM
        );
    }

    @Test
    void createIndexOnNestedFields() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT Y(a bigint, b bigint)" +
                "CREATE TYPE AS STRUCT X(s Y)" +
                "CREATE TABLE T1(col1 bigint, r X, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT r.s.a, r.s.b FROM T1 order by r.s.a, r.s.b";
        indexIs(stmt,
                field("R").nest(field("S").nest(concat(field("A"), field("B")))),
                IndexTypes.VALUE
        );
    }

    @Test
    void createIndexOnDeeplyNestedFields() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(b B)" +
                "CREATE TYPE AS STRUCT B(c C)" +
                "CREATE TYPE AS STRUCT C(d D)" +
                "CREATE TYPE AS STRUCT D(e E)" +
                "CREATE TYPE AS STRUCT E(f F)" +
                "CREATE TYPE AS STRUCT F(g G)" +
                "CREATE TYPE AS STRUCT G(x bigint, y bigint)" +
                "CREATE TABLE T1(col1 bigint, a A, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT a.b.c.d.e.f.g.x, a.b.c.d.e.f.g.y from T1 order by a.b.c.d.e.f.g.y";
        indexIs(stmt,
                keyWithValue(
                        field("A")
                                .nest(field("B")
                                        .nest(field("C")
                                                .nest(field("D")
                                                        .nest(field("E")
                                                                .nest(field("F")
                                                                        .nest(field("G")
                                                                                .nest(
                                                                                        concat(
                                                                                                field("Y"),
                                                                                                field("X"))))))))),
                        1),
                IndexTypes.VALUE);
    }

    @Test
    void createSimpleVersionIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT \"__ROW_VERSION\" FROM T1 ORDER BY \"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, version(), IndexTypes.VERSION);
    }

    @Test
    void createVersionIndexWithAliasedTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT t.\"__ROW_VERSION\" FROM T1 AS t ORDER BY t.\"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, version(), IndexTypes.VERSION);
    }

    @Test
    void failToCreateVersionIndexWithUnknownTable() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT t2.\"__ROW_VERSION\" FROM T1 AS t ORDER BY t2.\"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=true)";
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN, "Attempting to query non existing column T2.__ROW_VERSION");
    }

    @Test
    void createCompoundVersionIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT col2, \"__ROW_VERSION\", col3, col4 FROM T1 ORDER BY col2, \"__ROW_VERSION\", col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, keyWithValue(concat(field("COL2"), version(), field("COL3"), field("COL4")), 3), IndexTypes.VERSION);
    }

    @Test
    void createVersionIndexWithVersionInValue() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, col3 bigint, col4 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT col2, \"__ROW_VERSION\", col3, col4 FROM T1 ORDER BY col2 " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, keyWithValue(concat(field("COL2"), version(), field("COL3"), field("COL4")), 1), IndexTypes.VERSION);
    }

    @Test
    void createVersionIndexWithNestingFields() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT a.col2, \"__ROW_VERSION\", a.col3, a.col4 FROM T1 ORDER BY a.col2, \"__ROW_VERSION\", a.col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        // In theory, this should be fine, as the nested value is not repeated, but this is currently not distinguished by the index generator
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Index with multiple disconnected references to the same column are not supported");
    }

    @Test
    void createVersionIndexWithRepeatedNested() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT t1.\"__ROW_VERSION\", X.col3, X.col4 FROM T1, (SELECT col3, col4 FROM T1.A) X ORDER BY t1.\"__ROW_VERSION\", X.col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, keyWithValue(concat(version(), field("A").nest(field("values", KeyExpression.FanType.FanOut).nest(concatenateFields("COL3", "COL4")))), 2), IndexTypes.VERSION);
    }

    /**
     * A predicate on an index that needs a synthetic type is rejected for now: the predicate would have to
     * be evaluated against the synthetic record rather than the stored one.
     */
    @Test
    void createIndexWithPredicateOverUnnestedSyntheticTypeIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.col5, X.col3 FROM T1, (SELECT col2, col3 FROM T1.A) X " +
                "WHERE T1.col5 > 10 ORDER BY X.col2, T1.col5, X.col3";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION,
                "a predicate is not supported on an index over an unnested synthetic type");
    }

    /**
     * The same columns and the same predicate as
     * {@link #createIndexWithPredicateOverUnnestedSyntheticTypeIsNotSupported()}, but with the two columns of
     * {@code X} made adjacent. That is expressible as a fan-out, so no synthetic type is needed and the
     * predicate is accepted — reordering the key alone decides whether the predicate is allowed.
     */
    @Test
    void createIndexWithPredicateIsSupportedWhenUnnestingNeedsNoSyntheticType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, X.col3, T1.col5 FROM T1, (SELECT col2, col3 FROM T1.A) X " +
                "WHERE T1.col5 > 10 ORDER BY X.col2, X.col3, T1.col5";
        indexIs(stmt, concat(field("A").nest(field("values", KeyExpression.FanType.FanOut)
                .nest(concatenateFields("COL2", "COL3"))), field("COL5")), IndexTypes.VALUE);
    }

    /**
     * The same predicate is fine when the shape does not need a synthetic type: one column per unnesting
     * keeps the index on the stored table with a fan-out.
     */
    @Test
    void createIndexWithPredicateOverUnnestingIsSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col3, T1.col5 FROM T1, (SELECT col3 FROM T1.A) X " +
                "WHERE T1.col5 > 10 ORDER BY X.col3, T1.col5";
        indexIs(stmt, concat(field("A").nest(field("values", KeyExpression.FanType.FanOut).nest("COL3")),
                field("COL5")), IndexTypes.VALUE);
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

    // ─── Chained unnesting: an array nested inside the elements of another array ───────────────

    private static final String CHAINED_SCHEMA = "CREATE SCHEMA TEMPLATE test_template " +
            "CREATE TYPE AS STRUCT Q(y bigint, y2 bigint) " +
            "CREATE TYPE AS STRUCT P(x bigint, x2 bigint, q Q array) " +
            "CREATE TABLE A(k bigint, p P array, primary key(k)) ";

    /**
     * Chained unnesting where the two columns are adjacent. Both are read through the outer unnesting, so
     * a single fan-out into {@code p} covers them and correlates {@code y} to the element that supplied
     * {@code x} — one entry per (p, q) pair. No synthetic type is needed.
     */
    @Test
    void createIndexWithChainedUnnestingAdjacentKeepsFanOut() throws Exception {
        final String stmt = CHAINED_SCHEMA +
                "CREATE INDEX mv1 AS SELECT b.x, c.y FROM A AS a, (select * from a.p) as b, (select * from b.q) as c " +
                "ORDER BY b.x, c.y";
        indexIs(stmt, field("P", KeyExpression.FanType.None)
                .nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut)
                        .nest(concat(field("X"), field("Q", KeyExpression.FanType.None)
                                .nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut)
                                        .nest(field("Y")))))), IndexTypes.VALUE);
    }

    /**
     * Chained unnesting split by a parent column. Neither innermost unnesting is itself split, but both
     * columns are read through the outer one, and with {@code a.k} between them that outer navigation
     * would have to be emitted twice — so a synthetic type is required.
     */
    @Test
    void createIndexWithChainedUnnestingSplitByParentUsesSyntheticType() throws Exception {
        final String stmt = CHAINED_SCHEMA +
                "CREATE INDEX mv1 AS SELECT b.x, a.k, c.y FROM A AS a, (select * from a.p) as b, (select * from b.q) as c " +
                "ORDER BY b.x, a.k, c.y";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 2, (parent, constituents) -> concat(
                field(constituents.get(0)).nest("X"),
                field(parent).nest("K"),
                field(constituents.get(1)).nest("Y")));
    }

    /**
     * Chained unnesting where the split is within the inner unnesting.
     */
    @Test
    void createIndexWithChainedUnnestingInnerSplitUsesSyntheticType() throws Exception {
        final String stmt = CHAINED_SCHEMA +
                "CREATE INDEX mv1 AS SELECT c.y, a.k, c.y2 FROM A AS a, (select * from a.p) as b, (select * from b.q) as c " +
                "ORDER BY c.y, a.k, c.y2";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 2, (parent, constituents) -> concat(
                field(constituents.get(1)).nest("Y"),
                field(parent).nest("K"),
                field(constituents.get(1)).nest("Y2")));
    }

    /**
     * Chained unnesting where the split is within the outer unnesting.
     */
    @Test
    void createIndexWithChainedUnnestingOuterSplitUsesSyntheticType() throws Exception {
        final String stmt = CHAINED_SCHEMA +
                "CREATE INDEX mv1 AS SELECT b.x, a.k, b.x2 FROM A AS a, (select * from a.p) as b, (select * from b.q) as c " +
                "ORDER BY b.x, a.k, b.x2";
        syntheticIndexIs(stmt, IndexTypes.VALUE, 2, (parent, constituents) -> concat(
                field(constituents.get(0)).nest("X"),
                field(parent).nest("K"),
                field(constituents.get(0)).nest("X2")));
    }

    @Test
    void createVersionIndexWithRepeatedNestedSplitByVersion() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.\"__ROW_VERSION\", X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.\"__ROW_VERSION\", X.col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        // As above, but the column separating X.col2 from X.col3/X.col4 is the row version.
        syntheticIndexIs(stmt, IndexTypes.VERSION, (parent, x) -> keyWithValue(concat(
                field(x).nest("COL2"),
                field(parent).nest(version()),
                field(x).nest("COL3"),
                field(x).nest("COL4")), 3));
    }

    @Test
    void createVersionIndexWithoutQualifyingTableName() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, \"__ROW_VERSION\" FROM T1, (SELECT col2 FROM T1.A) X ORDER BY X.col2, \"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=true)";
        indexIs(stmt, concat(field("A").nest(field("values", KeyExpression.FanType.FanOut).nest("COL2")), version()), IndexTypes.VERSION);
    }

    @Test
    void versionIndexWithoutStoreRowVersions() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT \"__ROW_VERSION\" FROM T1 ORDER BY \"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=false)";
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN, "Attempting to query non existing column __ROW_VERSION");
    }

    @Test
    void failToCreateVersionIndexWithAmbiguousColumn() throws Exception {
        // Attempt to create a join index with a version column that doesn't specify which table the version comes from,
        // which results in an ambiguous column reference (regardless of join support writ large)
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key (col1)) " +
                "CREATE TABLE T2(col2 bigint, primary key (col2)) " +
                "CREATE INDEX mv1 AS SELECT \"__ROW_VERSION\", T1.col1, T2.col2 FROM T1, T2 ORDER BY \"__ROW_VERSION\", T1.col1, T2.col2 " +
                "WITH OPTIONS(store_row_versions=true)";
        shouldFailWith(stmt, ErrorCode.AMBIGUOUS_COLUMN, "Ambiguous reference __ROW_VERSION");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMax(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT %s(col2) FROM T1 group by col1", index);
        indexIs(stmt,
                field("COL2").groupBy(field("COL1")),
                "MIN".equals(index) ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                idx -> Assertions.assertEquals("0", idx.getOptions().get("permutedSize"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithGroupingOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, %s(col2) FROM T1 group by col1 order by col1", index);
        indexIs(stmt,
                field("COL2").groupBy(field("COL1")),
                "MIN".equals(index) ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                idx -> Assertions.assertEquals("0", idx.getOptions().get("permutedSize"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithGroupingOrderingIncludingMax(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, %s(col2) FROM T1 group by col1 order by col1, %s(col2)", index, index);
        indexIs(stmt,
                field("COL2").groupBy(field("COL1")),
                "MIN".equals(index) ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                idx -> Assertions.assertEquals("0", idx.getOptions().get("permutedSize"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithPermutedOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, col3, %s(col4) FROM T1 group by col1, col2, col3 order by col1, col2, %s(col4), col3", index, index);
        indexIs(stmt,
                field("COL4").groupBy(concatenateFields("COL1", "COL2", "COL3")),
                "MIN".equals(index) ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                idx -> Assertions.assertEquals("1", idx.getOptions().get("permutedSize"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnSourceOnMinMaxWithPermutedOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE VIEW v1 AS SELECT col1, col2, col3, %s(col4) as agg FROM T1 group by col1, col2, col3 ", index) +
                "CREATE INDEX mv1 ON v1(col1, col2, agg, col3)";
        indexIs(stmt,
                field("COL4").groupBy(concatenateFields("COL1", "COL2", "COL3")),
                "MIN".equals(index) ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                idx -> Assertions.assertEquals("1", idx.getOptions().get("permutedSize"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithGroupingColumnsMissingInOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, col3, %s(col4) FROM T1 group by col1, col2, col3 order by col1, %s(col4), col3", index, index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, attempt to create a covering aggregate index");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithMultipleAggregatesInOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, col3, %s(col4) FROM T1 group by col1, col2, col3 order by col1, %s(col4), %s(col4), col3", index, index, index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, aggregate can appear only once in ordering clause");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithFinalGroupingColumnsMissingInOrdering(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, col3, %s(col4) FROM T1 group by col1, col2, col3 order by col1, col2, %s(col4)", index, index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, attempt to create a covering aggregate index");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxWithGroupingColumnsMissingInResultColumn(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col3, %s(col4) FROM T1 group by col1, col2, col3 order by col1, col2, %s(col4), col3", index, index);
        shouldFailWith(stmt, ErrorCode.INVALID_COLUMN_REFERENCE, "Cannot create index and order by an expression that is not present in the projection list");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexWithGroupingColumnMissingInResults(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, %s(col4) FROM T1 group by col1, col2, col3", index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Grouping value absent from aggregate result value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexWithGroupingColumnsNotMatchingResultOrder(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col3, col2, %s(col4) FROM T1 group by col1, col2, col3", index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Aggregate result value does not align with grouping value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexWithExtraResultColumnsNotInGrouping(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, col4 bigint, primary key(col1)) " +
                String.format(Locale.ROOT, "CREATE INDEX mv1 AS SELECT col1, col2, col3, %s(col4) FROM T1 group by col1, col2", index);
        shouldFailWith(stmt, ErrorCode.GROUPING_ERROR, "Invalid reference to non-grouping expression T1.COL3");
    }

    @Test
    void createCountStarIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(*) FROM T1 group by col1";
        indexIs(stmt,
                new GroupingKeyExpression(field("COL1"), 0),
                IndexTypes.COUNT
        );
    }

    @Test
    void createCountCol() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(col1) FROM T1 group by col1";
        indexIs(stmt,
                field("COL1").groupBy(field("COL1")),
                IndexTypes.COUNT_NOT_NULL
        );
    }

    @Test
    void createMinEverLong() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MIN_EVER(col1) FROM T1 group by col2";
        indexIs(stmt,
                field("COL1").groupBy(field("COL2")),
                IndexTypes.MIN_EVER_TUPLE
        );
    }

    @Test
    void createMaxEverLong() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MAX_EVER(col1) FROM T1 group by col2";
        indexIs(stmt,
                field("COL1").groupBy(field("COL2")),
                IndexTypes.MAX_EVER_TUPLE
        );
    }

    @Test
    void createMaxEverTupleIncorrectType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MAX_EVER(col2) FROM T1 group by col1";
        indexIs(stmt,
                field("COL2").groupBy(field("COL1")),
                IndexTypes.MAX_EVER_TUPLE
        );
    }

    @Test
    void createMinEverTupleIncorrectType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MIN_EVER(col2) FROM T1 group by col1";
        indexIs(stmt,
                field("COL2").groupBy(field("COL1")),
                IndexTypes.MIN_EVER_TUPLE
        );
    }

    @Test
    void createMaxEverLongIncorrectType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MAX_EVER(col2) FROM T1 group by col1 WITH ATTRIBUTES LEGACY_EXTREMUM_EVER";
        shouldFailWith(stmt, ErrorCode.INTERNAL_ERROR, "only numeric types allowed in max_ever_long aggregation operation");
    }

    @Test
    void createMinEverLongIncorrectType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 string, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MIN_EVER(col2) FROM T1 group by col1 WITH ATTRIBUTES LEGACY_EXTREMUM_EVER";
        shouldFailWith(stmt, ErrorCode.INTERNAL_ERROR, "only numeric types allowed in min_ever_long aggregation operation");
    }

    @Test
    void createIndexWithOrderByInFromSelect() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.x from T AS t, (select M.x from t.a AS M order by M.x) SQ";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "order by is not supported in subquery");
    }

    @Test
    void createIndexWithOrderByInExistsSelect() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT t.p from T AS t where exists (select M.x from t.a AS M order by M.x)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "order by is not supported in subquery");
    }

    @Test
    void createIndexWithOrderByExpression() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x bigint) " +
                "CREATE TABLE T(p bigint, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT t.p from T AS t order by t.p + 4";
        shouldFailWith(stmt, ErrorCode.INVALID_COLUMN_REFERENCE, "Cannot create index and order by an expression that is not present in the projection list");
    }

    @Test
    void createIndexOnArrayFieldWithoutUnnestingIsDisallowed() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T (p BIGINT, items STRING ARRAY, PRIMARY KEY (p))" +
                "CREATE INDEX MV1 AS SELECT items FROM T";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "cannot create index on array field");
    }

    @Test
    void createIndexNavigatingThroughArrayWithoutUnnestingIsDisallowed() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(x BIGINT) " +
                "CREATE TABLE T (p BIGINT, a A ARRAY, PRIMARY KEY (p))" +
                "CREATE INDEX MV1 AS SELECT a.x FROM T";
        shouldFailWith(stmt, ErrorCode.UNDEFINED_COLUMN, "A.X");
    }

    @Test
    void createIndexWithOrderByMixedDirection() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, col2 bigint, col3 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT col1, col2, col3 FROM T1 ORDER BY col1 ASC, col2 DESC, col3 NULLS LAST";
        indexIs(stmt,
                concat(field("COL1"), function("order_desc_nulls_last", field("COL2")), function("order_asc_nulls_last", field("COL3"))),
                IndexTypes.VALUE);
    }

    @Test
    void createVectorIndexWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, z bigint, primary key(p))" +
                "CREATE VIEW V1 AS SELECT p, b, c, z from T where c > 50 " +
                "CREATE VECTOR INDEX MV1 USING HNSW ON V1(b) PARTITION BY(z)";
        indexIs(stmt,
                keyWithValue(concat(field("Z"), field("B")), 1),
                IndexTypes.VECTOR,
                idx -> {
                    final var predicate = idx.getPredicate();
                    assertThat(predicate).isEqualTo(Predicate.newBuilder()
                            .setValuePredicate(ValuePredicate
                                    .newBuilder()
                                    .addValue("C")
                                    .setComparison(Comparison
                                            .newBuilder()
                                            .setSimpleComparison(SimpleComparison.newBuilder()
                                                    .setType(ComparisonType.GREATER_THAN)
                                                    .setOperand(Value.newBuilder().setLongValue(50).build())
                                                    .build())
                                            .build())
                                    .build())
                            .build());
                });
    }


    @Test
    void createVectorIndexWithoutPartitionClauseWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, z bigint, primary key(p))" +
                "CREATE VIEW V1 AS SELECT p, b, c, z from T " +
                "CREATE VECTOR INDEX MV1 USING HNSW ON V1(b)";
        indexIs(stmt,
                keyWithValue(field("B"), 0),
                IndexTypes.VECTOR);
    }

    @Test
    void createVectorIndexWithOptionsWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) " +
                "OPTIONS (CONNECTIVITY = 16, M_MAX = 32, EF_CONSTRUCTION = 200, METRIC = COSINE_METRIC)";
        indexIs(stmt,
                keyWithValue(concat(field("P"), field("B")), 1),
                IndexTypes.VECTOR,
                idx -> {
                    final var options = idx.getOptions();
                    Assertions.assertEquals("3", options.get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals("16", options.get(IndexOptions.HNSW_M));
                    Assertions.assertEquals("32", options.get(IndexOptions.HNSW_M_MAX));
                    Assertions.assertEquals("200", options.get(IndexOptions.HNSW_EF_CONSTRUCTION));
                    Assertions.assertEquals("COSINE_METRIC", options.get(IndexOptions.HNSW_METRIC));
                    // and the same values read back through the typed option keys
                    final var coreIndex = toCoreIndex(idx);
                    Assertions.assertEquals(3, VectorIndexOptionKeys.NUM_DIMENSIONS.read(coreIndex));
                    Assertions.assertEquals(16, VectorIndexOptionKeys.HNSW_M.read(coreIndex));
                    Assertions.assertEquals(32, VectorIndexOptionKeys.HNSW_M_MAX.read(coreIndex));
                    Assertions.assertEquals(200, VectorIndexOptionKeys.HNSW_EF_CONSTRUCTION.read(coreIndex));
                    Assertions.assertEquals(Metric.COSINE_METRIC, VectorIndexOptionKeys.METRIC.read(coreIndex));
                    validateVectorIndex(idx);
                });
    }

    @Test
    void createVectorIndexWithRabitQOptionsWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(128, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) " +
                "OPTIONS (USE_RABITQ = true, RABITQ_NUM_EX_BITS = 4, MAINTAIN_STATS_PROBABILITY = 0.01)";
        indexIs(stmt,
                keyWithValue(concat(field("P"), field("B")), 1),
                IndexTypes.VECTOR,
                idx -> {
                    final var options = idx.getOptions();
                    Assertions.assertEquals("128", options.get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals("true", options.get(IndexOptions.HNSW_USE_RABITQ));
                    Assertions.assertEquals("4", options.get(IndexOptions.HNSW_RABITQ_NUM_EX_BITS));
                    Assertions.assertEquals("0.01", options.get(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY));
                    // and the same values read back through the typed option keys
                    final var coreIndex = toCoreIndex(idx);
                    Assertions.assertEquals(128, VectorIndexOptionKeys.NUM_DIMENSIONS.read(coreIndex));
                    Assertions.assertEquals(true, VectorIndexOptionKeys.USE_RABITQ.read(coreIndex));
                    Assertions.assertEquals(4, VectorIndexOptionKeys.RABITQ_NUM_EX_BITS.read(coreIndex));
                    Assertions.assertEquals(0.01, VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY.read(coreIndex));
                    validateVectorIndex(idx);
                });
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 16, 256, 1024})
    void createVectorIndexWithVariousDimensionsWorksCorrectly(int dimensions) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(" + dimensions + ", float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p)";
        indexIs(stmt, keyWithValue(concat(field("P"), field("B")), 1), IndexTypes.VECTOR,
                idx -> {
                    Assertions.assertEquals(String.valueOf(dimensions), idx.getOptions().get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals(dimensions, VectorIndexOptionKeys.NUM_DIMENSIONS.read(toCoreIndex(idx)));
                    validateVectorIndex(idx);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"EUCLIDEAN_METRIC", "EUCLIDEAN_SQUARE_METRIC", "DOT_PRODUCT_METRIC", "COSINE_METRIC"})
    void createVectorIndexWithAllMetricTypesWorksCorrectly(String metric) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(512, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) OPTIONS (METRIC = " + metric + ")";

        indexIs(stmt, keyWithValue(concat(field("P"), field("B")), 1), IndexTypes.VECTOR,
                idx -> {
                    Assertions.assertEquals("512", idx.getOptions().get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals(metric, idx.getOptions().get(IndexOptions.HNSW_METRIC));
                    final var coreIndex = toCoreIndex(idx);
                    Assertions.assertEquals(512, VectorIndexOptionKeys.NUM_DIMENSIONS.read(coreIndex));
                    Assertions.assertEquals(Metric.valueOf(metric), VectorIndexOptionKeys.METRIC.read(coreIndex));
                    // Validate using VectorIndexMaintainerFactory validator
                    validateVectorIndex(idx);
                });
    }

    private void validateVectorIndex(RecordLayerIndex recordLayerIndex) {
        final var coreIndex = toCoreIndex(recordLayerIndex);

        // Validate using VectorIndexHelper - this validates the configuration options
        // VectorIndexHelper.validate() will throw if options are invalid
        Assertions.assertDoesNotThrow(() -> VectorIndexHelper.validate(coreIndex),
                "Vector index configuration should be valid");
    }

    /**
     * Converts a {@link RecordLayerIndex} to a core {@link com.apple.foundationdb.record.metadata.Index} so its options
     * can be read back through the typed {@link VectorIndexOptionKeys} (whose {@code read} operates on a core index).
     */
    private com.apple.foundationdb.record.metadata.Index toCoreIndex(RecordLayerIndex recordLayerIndex) {
        return new com.apple.foundationdb.record.metadata.Index(
                recordLayerIndex.getName(),
                recordLayerIndex.getKeyExpression(),
                recordLayerIndex.getIndexType(),
                recordLayerIndex.getOptions(),
                recordLayerIndex.getPredicate() != null
                        ? com.apple.foundationdb.record.metadata.IndexPredicate.fromProto(recordLayerIndex.getPredicate())
                        : null
        );
    }

    @Test
    void createVectorIndexWithStatsOptionsWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(64, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) " +
                "OPTIONS (SAMPLE_VECTOR_STATS_PROBABILITY = 0.05)";
        indexIs(stmt,
                keyWithValue(concat(field("P"), field("B")), 1),
                IndexTypes.VECTOR,
                idx -> {
                    final var options = idx.getOptions();
                    Assertions.assertEquals("64", options.get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals("0.05", options.get(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY));
                    // and the same values read back through the typed option keys
                    final var coreIndex = toCoreIndex(idx);
                    Assertions.assertEquals(64, VectorIndexOptionKeys.NUM_DIMENSIONS.read(coreIndex));
                    Assertions.assertEquals(0.05, VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY.read(coreIndex));
                    validateVectorIndex(idx);
                });
    }

    @Test
    void createGuardiannVectorIndexWithOptionsWorksCorrectly() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING GUARDIANN ON T(b) PARTITION BY (p) " +
                "OPTIONS (METRIC = COSINE_METRIC, PRIMARY_CLUSTER_MIN = 20, PRIMARY_CLUSTER_MAX = 200, " +
                "REPLICATED_CLUSTER_TARGET = 50, REPLICATION_PRIORITY_MIN = 0.75, COLLAPSE_MIN_DUPLICATES = 100)";
        indexIs(stmt,
                keyWithValue(concat(field("P"), field("B")), 1),
                IndexTypes.VECTOR,
                idx -> {
                    final var options = idx.getOptions();
                    Assertions.assertEquals("GUARDIANN", options.get(IndexOptions.VECTOR_ENGINE));
                    // shared options are written under their (currently canonical) hnsw* wire names
                    Assertions.assertEquals("3", options.get(IndexOptions.HNSW_NUM_DIMENSIONS));
                    Assertions.assertEquals("COSINE_METRIC", options.get(IndexOptions.HNSW_METRIC));
                    Assertions.assertEquals("20", options.get(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN));
                    Assertions.assertEquals("200", options.get(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX));
                    Assertions.assertEquals("50", options.get(IndexOptions.GUARDIANN_REPLICATED_CLUSTER_TARGET));
                    Assertions.assertEquals("0.75", options.get(IndexOptions.GUARDIANN_REPLICATION_PRIORITY_MIN));
                    Assertions.assertEquals("100", options.get(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES));
                    // and the same values read back through the typed option keys
                    final var coreIndex = toCoreIndex(idx);
                    Assertions.assertEquals(3, VectorIndexOptionKeys.NUM_DIMENSIONS.read(coreIndex));
                    Assertions.assertEquals(Metric.COSINE_METRIC, VectorIndexOptionKeys.METRIC.read(coreIndex));
                    Assertions.assertEquals(20, VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MIN.read(coreIndex));
                    Assertions.assertEquals(200, VectorIndexOptionKeys.GUARDIANN_PRIMARY_CLUSTER_MAX.read(coreIndex));
                    Assertions.assertEquals(50, VectorIndexOptionKeys.GUARDIANN_REPLICATED_CLUSTER_TARGET.read(coreIndex));
                    Assertions.assertEquals(0.75, VectorIndexOptionKeys.GUARDIANN_REPLICATION_PRIORITY_MIN.read(coreIndex));
                    Assertions.assertEquals(100, VectorIndexOptionKeys.GUARDIANN_COLLAPSE_MIN_DUPLICATES.read(coreIndex));
                    validateVectorIndex(idx);
                });
    }

    @Test
    void createGuardiannVectorIndexWithHnswOnlyOptionIsRejected() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING GUARDIANN ON T(b) PARTITION BY (p) OPTIONS (CONNECTIVITY = 16)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "not valid for the GUARDIANN vector engine");
    }

    @Test
    void createGuardiannVectorIndexWithNonNumericOptionValueIsRejected() throws Exception {
        // PRIMARY_CLUSTER_MAX expects an integer. 1.5 is a valid vectorIndexOptionValue (a REAL_LITERAL) but cannot be
        // coerced to an int, so option parsing surfaces a syntax error rather than letting the NumberFormatException
        // escape.
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING GUARDIANN ON T(b) PARTITION BY (p) OPTIONS (PRIMARY_CLUSTER_MAX = 1.5)";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR, "invalid value");
    }

    @Test
    void createHnswVectorIndexWithGuardiannOnlyOptionIsRejected() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) OPTIONS (PRIMARY_CLUSTER_MIN = 20)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "not valid for the HNSW vector engine");
    }

    @Test
    void createVectorIndexWithUnknownOptionIsRejected() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p) OPTIONS (BOGUS_OPTION = 5)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "unsupported vector index option 'bogus_option'");
    }

    @Test
    void createVectorIndexOnMultipleColumnsFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c vector(3, float), primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b, c) PARTITION BY (p)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "invalid number of indexed columns, only one column is supported");
    }

    @Test
    void createVectorIndexOnNonVectorColumnFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b bigint, primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p)";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR, "indexed column must be of vector type");
    }

    @Test
    void createVectorIndexOnStringColumnFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b string, primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) PARTITION BY (p)";
        shouldFailWith(stmt, ErrorCode.SYNTAX_ERROR, "indexed column must be of vector type");
    }

    @Test
    void createVectorIndexWithIncludeClauseFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, d string, primary key(p)) " +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) INCLUDE (c, d) PARTITION BY (p) ";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "INCLUDE clause is not supported for vector indexes");
    }

    @Test
    void createVectorIndexWithIncludeClauseAndPartitionFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, z bigint, primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) INCLUDE (c) PARTITION BY(z)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "INCLUDE clause is not supported for vector indexes");
    }

    @Test
    void createVectorIndexWithIncludeClauseAndOptionsFails() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T(p bigint, b vector(3, float), c bigint, primary key(p))" +
                "CREATE VECTOR INDEX MV1 USING HNSW ON T(b) INCLUDE (c) PARTITION BY (p) OPTIONS (CONNECTIVITY = 16)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "INCLUDE clause is not supported for vector indexes");
    }
}
