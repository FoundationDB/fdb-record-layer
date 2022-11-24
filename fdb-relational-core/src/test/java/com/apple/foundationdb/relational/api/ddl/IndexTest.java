/*
 * IndexTest.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.TableInfo;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpConstantActionFactory;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import com.apple.foundationdb.relational.util.NullableArrayUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.relational.util.NullableArrayUtils.REPEATED_FIELD_NAME;

public class IndexTest {
    @BeforeAll
    public static void setup() {
        if (Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    private final PlanContext fakePlanContext;

    public IndexTest() throws RelationalException {
        TypingContext ctx = TypingContext.create();
        SystemTableRegistry.getSystemTable("SCHEMAS").addDefinition(ctx);
        SystemTableRegistry.getSystemTable("DATABASES").addDefinition(ctx);
        ctx.addAllToTypeRepository();
        RecordMetaDataProto.MetaData md = ctx.generateSchemaTemplate("CATALOG_TEMPLATE", 1L).generateSchema("__SYS", "CATALOG").getMetaData();
        fakePlanContext = PlanContext.Builder.create()
                .withMetadata(RecordMetaData.build(md))
                .withStoreState(new RecordStoreState(RecordMetaDataProto.DataStoreInfo.newBuilder().build(), null))
                .withDbUri(URI.create("/IndexTest"))
                .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                .withConstantActionFactory(NoOpConstantActionFactory.INSTANCE)
                .build();
    }

    void shouldFailWith(@Nonnull final String query, @Nonnull ErrorCode errorCode, @Nonnull final String errorMessage) throws Exception {
        shouldFailWithInjectedFactory(query, errorCode, errorMessage, fakePlanContext.getConstantActionFactory());
    }

    void shouldFailWithInjectedFactory(@Nonnull final String query,
                                       @Nonnull ErrorCode errorCode,
                                       @Nonnull final String errorMessage,
                                       @Nonnull ConstantActionFactory constantActionFactory) throws Exception {
        final RelationalException ve = Assertions.assertThrows(RelationalException.class, () ->
                Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withConstantActionFactory(constantActionFactory).build()));
        Assertions.assertEquals(errorCode, ve.getErrorCode());
        Assertions.assertTrue(ve.getMessage().contains(errorMessage), String.format("expected error message '%s' to contain '%s' but it didn't", ve.getMessage(), errorMessage));
    }

    void shouldWorkWithInjectedFactory(@Nonnull final String query, @Nonnull ConstantActionFactory constantActionFactory) throws Exception {
        Plan.generate(query, PlanContext.Builder.unapply(fakePlanContext).withConstantActionFactory(constantActionFactory).build());
    }

    private void indexIs(@Nonnull String stmt, @Nonnull final KeyExpression expectedKey, @Nonnull final String indexType) throws Exception {
        shouldWorkWithInjectedFactory(stmt, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, template.getTables().size(), "Incorrect number of tables");
                final TableInfo info = template.getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final RecordMetaDataProto.Index index = info.getIndexes().get(0);
                Assertions.assertEquals("MV1", index.getName(), "Incorrect index name!");
                Assertions.assertEquals(indexType, index.getType());
                final KeyExpression actualKey = KeyExpression.fromProto(index.getRootExpression());
                Assertions.assertEquals(expectedKey, actualKey);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createdIndexWorksSimpleNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64, y int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.F from T AS t, (select M.x as F from t.a AS M) SQ";
        indexIs(stmt, field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksSimpleNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p)) " +
                "CREATE INDEX mv1 AS SELECT SQ.x, t.p from T AS t, (select M.x from t.a AS M) SQ order by SQ.x, t.p";
        indexIs(stmt, concat(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))), field("P")), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksSimpleNestingAndConcatDifferentOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT t.p, SQ.x from T AS t, (select M.x from t.a AS M) SQ ORDER BY t.p, SQ.x";
        indexIs(stmt, concat(field("P"), field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None)))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64, pp int64) " +
                "CREATE STRUCT B(a A array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.x from T AS t, (select M.x from t.b AS Y, (select x, pp from Y.a) M) SQ";
        indexIs(stmt, field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))))), IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT C(z int64) " +
                "CREATE STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ1.x,SQ2.z from " +
                "  T AS t," +
                "  (select M.x from t.b AS Y, (select x from Y.a) M) SQ1," +
                "  (select M.z from t.b AS Y, (select z from Y.c) M) SQ2" +
                " ORDER BY SQ1.x, SQ2.z";
        indexIs(stmt,
                concat(field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))))),
                        field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("C", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("Z", KeyExpression.FanType.None)))))),
                IndexTypes.VALUE);
    }

    @Test
    void createdIndexWorksDeepNestingAndConcatCartesian() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT C(z int64, k int64) " +
                "CREATE STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
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
    void createdIndexWorksDeepNestingAndNestedCartesianConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT C(z int64) " +
                "CREATE STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT SQ.x, SQ.z from T AS t, (select M.x, N.z from t.b AS Y, (select x from Y.a) M, (select z from Y.c) N) SQ ORDER BY SQ.x, SQ.z";
        indexIs(stmt,
                field("B", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(
                        concat(field("A", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("X", KeyExpression.FanType.None))),
                                field("C", KeyExpression.FanType.None).nest(field(NullableArrayUtils.getRepeatedFieldName(), KeyExpression.FanType.FanOut).nest(field("Z", KeyExpression.FanType.None)))
                        ))),
                IndexTypes.VALUE);
    }

    @Test
    void createIndexWithPredicateIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T(p int64, a A array, b B array, primary key(p))" +
                "CREATE INDEX mv1 AS SELECT * FROM T where p > 10 order by p, a, b";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found predicate");
    }

    @Test
    void createIndexWithImproperNestedFieldClusteringIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 int64, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT X.a1,Y.b2,X.c1 FROM (SELECT a1,c1 FROM T1) X, (SELECT b2 FROM T2) Y order by x.a1, y.b2, x.c1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, more than one iteration generator found");
    }

    @Test
    void createIndexWithJoiningMoreThanOneTableIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 int64, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT * FROM T1, T2 order by t1.p1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, more than one iteration generator found");
    }

    @Test
    void createIndexWithExpressionsInProjectionIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT 5+1 FROM T1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, not all fields can be mapped to key expression in");
    }

    @Test
    void createSimpleValueIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1 FROM T1";
        indexIs(stmt,
                field("A1"),
                IndexTypes.VALUE
        );
    }

    @Test
    void createSimpleValueIndexOnTwoCols() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a1, a2";
        indexIs(stmt,
                concat(field("A1"), field("A2")),
                IndexTypes.VALUE);
    }

    @Test
    void createSimpleValueIndexOnNestedCol() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT S1(S1_1 int64, S1_2 int64) " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 S1, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a2.S1_1 FROM T1 order by a2.S1_1";
        indexIs(stmt, field("A2").nest(field("S1_1")),
                IndexTypes.VALUE);
    }

    @Test
    void createSimpleValueIndexOnTwoColsReverse() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a2, a1";
        indexIs(stmt,
                concat(field("A2"), field("A1")),
                IndexTypes.VALUE);
    }

    @Test
    void createCoveringValueIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, a3 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2, a3 FROM T1 order by a1, a2";
        indexIs(stmt,
                keyWithValue(concat(field("A1"), field("A2"), field("A3")), 2),
                IndexTypes.VALUE
        );
    }

    @Test
    void createIndexWithoutTopOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "indexes must have an order by clause at the top level");
    }

    @Test
    void createIndexOrderByUnknownColumns() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1, a2 FROM T1 order by a4";
        shouldFailWith(stmt, ErrorCode.INVALID_COLUMN_REFERENCE, "non existing column");
    }

    @Test
    void createIndexOrderByUnprojectedColumn() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(p1 int64, a1 int64, a2 int64, primary key(p1)) " +
                "CREATE INDEX mv1 AS SELECT a1 FROM T1 order by a2";
        shouldFailWith(stmt, ErrorCode.INVALID_COLUMN_REFERENCE, "not present in the projection list");
    }

    @Test
    void createIndexWithImproperNestedFieldClusteringInOrderByIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 int64, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE INDEX mv1 AS SELECT X.a1,X.c1, Y.b2 FROM (SELECT a1,c1 FROM T1) X, (SELECT b2 FROM T2) Y order by x.a1, y.b2, x.c1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, more than one iteration generator found");
    }

    @Test
    void createAggregateIndexWithGroupByContainingMoreThanOneAggregationIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, col3 int64, col4 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2), COUNT(col2) FROM T1 GROUP BY col3, col4";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found group by expression with more than one aggregation");
    }

    @Test
    void createNestedAggregateIndexIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, col3 int64, col4 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(h) FROM (SELECT sum(col2) as H FROM T1 GROUP BY col1) as x";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by expressions found");
    }

    @Test
    void multipleSelectsOverGroupBy() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, col3 int64, col4 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT * FROM (SELECT * FROM (SELECT count(col2), sum(col2) from t1 group by col3, col4) B) A";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found group by expression with more than one aggregation");
    }

    @Test
    void createIndexAsSelectWithGroupByWorks() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, col3 int64, col4 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2), col3, col4 FROM T1 GROUP BY col3, col4";
        indexIs(stmt,
                field("COL2").groupBy(field("COL3"), field("COL4")),
                IndexTypes.SUM
        );
    }

    @Test
    void createIndexAsSelectWithGroupByWithoutExplicitProjectionOfGroupingValuesWorks() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, col3 int64, col4 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT SUM(col2) FROM T1 GROUP BY col3, col4";
        indexIs(stmt,
                field("COL2").groupBy(field("COL3"), field("COL4")),
                IndexTypes.SUM
        );
    }

    @Test
    void createIndexOnNestedFields() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT Y(a int64, b int64)" +
                "CREATE STRUCT X(s Y)" +
                "CREATE TABLE T1(col1 int64, r X, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT r.s.a, r.s.b FROM T1 order by r.s.a, r.s.b";
        indexIs(stmt,
                field("R").nest(field("S").nest(concat(field("A"), field("B")))),
                IndexTypes.VALUE
        );
    }

    @Test
    void createIndexOnDeeplyNestedFields() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(b B)" +
                "CREATE STRUCT B(c C)" +
                "CREATE STRUCT C(d D)" +
                "CREATE STRUCT D(e E)" +
                "CREATE STRUCT E(f F)" +
                "CREATE STRUCT F(g G)" +
                "CREATE STRUCT G(x int64, y int64)" +
                "CREATE TABLE T1(col1 int64, a A, primary key(col1)) " +
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

    @ParameterizedTest
    @ValueSource(strings = {"MIN", "MAX"})
    void createAggregateIndexOnMinMaxIsNotSupport(String index) throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, primary key(col1)) " +
                String.format("CREATE INDEX mv1 AS SELECT %s(col2) FROM T1 group by col1", index);
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported aggregate index definition containing non-indexable aggregation");
    }

    @Test
    void createCountStarIndex() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(*) FROM T1 group by col1";
        indexIs(stmt,
                new GroupingKeyExpression(field("COL1"), 0),
                IndexTypes.COUNT
        );
    }

    @Test
    void createCountCol() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT COUNT(col1) FROM T1 group by col1";
        indexIs(stmt,
                field("COL1").groupBy(field("COL1")),
                IndexTypes.COUNT_NOT_NULL
        );
    }

    @Test
    void createMinEverLong() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MIN_EVER(col1) FROM T1 group by col2";
        indexIs(stmt,
                field("COL1").groupBy(field("COL2")),
                IndexTypes.MIN_EVER_LONG
        );
    }

    @Test
    void createMaxEverLong() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 int64, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MAX_EVER(col1) FROM T1 group by col2";
        indexIs(stmt,
                field("COL1").groupBy(field("COL2")),
                IndexTypes.MAX_EVER_LONG
        );
    }

    @Test
    void createMaxEverLongIncorrectType() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE T1(col1 int64, col2 string, primary key(col1)) " +
                "CREATE INDEX mv1 AS SELECT MAX_EVER(col2) FROM T1 group by col1";
        shouldFailWith(stmt, ErrorCode.INTERNAL_ERROR, "unknown reason only numeric types allowed in max_ever_long aggregation operation");
    }
}
