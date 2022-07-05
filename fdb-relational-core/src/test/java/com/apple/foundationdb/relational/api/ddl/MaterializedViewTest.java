/*
 * MaterializedViewTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public class MaterializedViewTest {
    @BeforeAll
    public static void setup() {
        if (Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    private final PlanContext fakePlanContext;

    private static final String[] validPrimitiveDataTypes = new String[]{
            "int64", "double", "boolean", "string", "bytes"
    };

    public MaterializedViewTest() throws RelationalException {
        TypingContext ctx = TypingContext.create();
        SystemTableRegistry.getSystemTable("SCHEMAS").addDefinition(ctx);
        SystemTableRegistry.getSystemTable("DATABASES").addDefinition(ctx);
        ctx.addAllToTypeRepository();
        RecordMetaDataProto.MetaData md = ctx.generateSchemaTemplate("catalog_template").generateSchema("__SYS", "catalog").getMetaData();
        fakePlanContext = PlanContext.Builder.create()
                .withMetadata(RecordMetaData.build(md))
                .withStoreState(new RecordStoreState(RecordMetaDataProto.DataStoreInfo.newBuilder().build(), null))
                .withDbUri(URI.create("/MaterializedViewTest"))
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

    private void matViewIs(@Nonnull String stmt, @Nonnull final KeyExpression expectedKey) throws Exception {
        shouldWorkWithInjectedFactory(stmt, new AbstractConstantActionFactory() {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template,
                                                                        @Nonnull Options templateProperties) {
                Assertions.assertEquals(1, template.getTables().size(), "Incorrect number of tables");
                final TableInfo info = template.getTables().stream().findFirst().orElseThrow();
                Assertions.assertEquals(1, info.getIndexes().size(), "Incorrect number of indexes!");
                final RecordMetaDataProto.Index index = info.getIndexes().get(0);
                Assertions.assertEquals("mv1", index.getName(), "Incorrect index name!");
                final KeyExpression actualKey = KeyExpression.fromProto(index.getRootExpression());
                Assertions.assertEquals(expectedKey, actualKey);
                return txn -> {
                };
            }
        });
    }

    @Test
    void createdMatViewWorksSimpleNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p)) " +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT SQ.x from T AS t, (select M.x from t.a AS M) SQ"
                ;
        matViewIs(stmt, field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None)));
    }

    @Test
    void createdMatViewWorksSimpleNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p)) " +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT SQ.x, t.p from T AS t, (select M.x from t.a AS M) SQ";
        matViewIs(stmt, concat(field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None)), field("p")));
    }

    @Test
    void createdMatViewWorksSimpleNestingAndConcatDifferentOrder() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE TABLE T(p int64, a A array, primary key(p))" +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT t.p, SQ.x from T AS t, (select M.x from t.a AS M) SQ";
        matViewIs(stmt, concat(field("p"), field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None))));
    }

    @Test
    void createdMatViewWorksDeepNesting() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(a A array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT SQ.x from T AS t, (select M.x from t.b AS Y, (select x from Y.a) M) SQ";
        matViewIs(stmt, field("b", KeyExpression.FanType.FanOut).nest(field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None))));
    }

    @Test
    void createdMatViewWorksDeepNestingAndConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT C(z int64) " +
                "CREATE STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT SQ1.x,SQ2.z from T AS t, (select M.x from t.b AS Y, (select x from Y.a) M) SQ1, (select M.z from t.b AS Y, (select z from Y.c) M) SQ2";
        matViewIs(stmt,
                concat(field("b", KeyExpression.FanType.FanOut).nest(field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None))),
                        field("b", KeyExpression.FanType.FanOut).nest(field("c", KeyExpression.FanType.FanOut).nest(field("z", KeyExpression.FanType.None)))));
    }

    @Test
    void createdMatViewWorksDeepNestingAndNestedCartesianConcat() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT C(z int64) " +
                "CREATE STRUCT B(a A array, c C array) " +
                "CREATE TABLE T(p int64, b B array, primary key(p))" +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT SQ.x, SQ.z from T AS t, (select M.x, N.z from t.b AS Y, (select x from Y.a) M, (select z from Y.c) N) SQ";
        matViewIs(stmt,
                field("b", KeyExpression.FanType.FanOut).nest(
                        concat(field("a", KeyExpression.FanType.FanOut).nest(field("x", KeyExpression.FanType.None)),
                                field("c", KeyExpression.FanType.FanOut).nest(field("z", KeyExpression.FanType.None)))
                ));
    }

    @Test
    void createMatViewWithPredicateIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T(p int64, a A array, b B array, primary key(p))" +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM T where p > 10";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found predicate");
    }

    @Test
    void createMatViewWithImproperNestedFieldClusteringIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 int64, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT X.a1,Y.b2,X.c1 FROM (SELECT a1,c1 FROM T1) X, (SELECT b2 FROM T2) Y";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, improper column clustering");
    }

    @Test
    void createMatViewWithJoiningMoreThanOneTableIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE TABLE T2(p2 int64, a2 A array, b2 B array, primary key(p2)) " +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM T1, T2";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found more than iteration generator");
    }

    @Test
    void createMatViewWithExpressionsInProjectionIsNotSupported() throws Exception {
        final String stmt = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE STRUCT A(x int64) " +
                "CREATE STRUCT B(y string) " +
                "CREATE TABLE T1(p1 int64, a1 A array, c1 B array, primary key(p1)) " +
                "CREATE MATERIALIZED VIEW mv1 AS SELECT 5+1 FROM T1";
        shouldFailWith(stmt, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, not all fields can be mapped to key expression in");
    }
}
