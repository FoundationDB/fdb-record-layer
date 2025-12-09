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

import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
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
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.RecordMetaDataProto.Comparison;
import static com.apple.foundationdb.record.RecordMetaDataProto.ComparisonType;
import static com.apple.foundationdb.record.RecordMetaDataProto.Predicate;
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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(DdlStatementParsingTest.class, TestSchemas.books());

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
                        "/IndexTest", metadataOperationsFactory).getPlan(query));
        connection.rollback();
        connection.setAutoCommit(true);
    }

    private void indexIs(@Nonnull final String stmt, @Nonnull final KeyExpression expectedKey, @Nonnull final String indexType) throws Exception {
        indexIs(stmt, expectedKey, indexType, index -> { });
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
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Index with multiple disconnected references to the same column are not supported");
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

    @Test
    void createVersionIndexWithRepeatedNestedSplitByVersion() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, T1.\"__ROW_VERSION\", X.col3, X.col4 FROM T1, (SELECT col2, col3, col4 FROM T1.A) X ORDER BY X.col2, T1.\"__ROW_VERSION\", X.col3 " +
                "WITH OPTIONS(store_row_versions=true)";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Index with multiple disconnected references to the same column are not supported");
    }

    @Test
    void failToCreateVersionIndexWithAmbiguousSource() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT X.col2, \"__ROW_VERSION\" FROM T1, (SELECT col2 FROM T1.A) X ORDER BY X.col2, \"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=true)";
        shouldFailWith(stmt, ErrorCode.AMBIGUOUS_COLUMN, "Ambiguous reference __ROW_VERSION");
    }

    @Test
    void versionIndexWithoutStoreRowVersions() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 bigint, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT \"__ROW_VERSION\" FROM T1 ORDER BY \"__ROW_VERSION\" " +
                "WITH OPTIONS(store_row_versions=false)";
        // TODO: it's possible the right thing here is to reject index creation because the meta-data is not configured to store versions
        indexIs(stmt, version(), IndexTypes.VERSION);
    }

    @Disabled // until REL-628 is in.
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
                    // Validate using VectorIndexMaintainerFactory validator
                    validateVectorIndex(idx);
                });
    }

    private void validateVectorIndex(RecordLayerIndex recordLayerIndex) {
        // Convert RecordLayerIndex to core Index
        final var coreIndex = new com.apple.foundationdb.record.metadata.Index(
                recordLayerIndex.getName(),
                recordLayerIndex.getKeyExpression(),
                recordLayerIndex.getIndexType(),
                recordLayerIndex.getOptions(),
                recordLayerIndex.getPredicate() != null
                        ? com.apple.foundationdb.record.metadata.IndexPredicate.fromProto(recordLayerIndex.getPredicate())
                        : null
        );

        // Validate using VectorIndexHelper - this validates the configuration options
        // VectorIndexHelper.getConfig() will throw IllegalArgumentException if options are invalid
        Assertions.assertDoesNotThrow(() ->
                com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexHelper.getConfig(coreIndex),
                "Vector index configuration should be valid");
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
                    validateVectorIndex(idx);
                });
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
