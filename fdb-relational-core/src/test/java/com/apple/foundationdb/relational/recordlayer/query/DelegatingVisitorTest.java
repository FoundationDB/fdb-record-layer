/*
 * DelegatingVisitorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.apple.foundationdb.relational.recordlayer.query.visitors.DelegatingVisitor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.stream.Stream;

public class DelegatingVisitorTest {

    static Stream<String> traversalStrings() {
        return Stream.of("LEVEL", "PREORDER");
    }

    @Nonnull
    private static RecordLayerSchemaTemplate generateMetadata() {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder(false)
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("R")
                                                .setDataType(DataType.Primitives.INTEGER.type())
                                                .build())
                                .setName("table1")
                                .build())
                .build();
    }

    @Test
    void visitPredicatedExpressionTest() {
        final var query = "X BETWEEN 32 AND 43";
        final MutableBoolean baseVisitorCalled = new MutableBoolean(false);
        final var baseVisitor = new BaseVisitor(
                new MutablePlanGenerationContext(PreparedParams.empty(),
                        PlanHashable.PlanHashMode.VC0, query, query, 42),
                generateMetadata(),
                NoOpQueryFactory.INSTANCE,
                NoOpMetadataOperationsFactory.INSTANCE,
                URI.create("/FDB/FRL1"),
                false) {
            @Nonnull
            @Override
            public Expression visitPredicatedExpression(@Nonnull final RelationalParser.PredicatedExpressionContext ctx) {
                baseVisitorCalled.setTrue();
                return Expression.ofUnnamed(LiteralValue.ofScalar(42));
            }
        };
        final var delegatingVisitor = new DelegatingVisitor<>(baseVisitor);
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var parser = new RelationalParser(new CommonTokenStream(tokenSource));
        final var predicatedExpression = (RelationalParser.PredicatedExpressionContext)parser.expression();
        delegatingVisitor.visitPredicatedExpression(predicatedExpression);
        Assertions.assertThat(baseVisitorCalled.booleanValue()).isTrue();
    }

    @Test
    void visitSubscriptExpressionTest() {
        final var query = "X[42]";
        final MutableBoolean baseVisitorCalled = new MutableBoolean(false);
        final var baseVisitor = new BaseVisitor(
                new MutablePlanGenerationContext(PreparedParams.empty(),
                        PlanHashable.PlanHashMode.VC0, query, query, 42),
                generateMetadata(),
                NoOpQueryFactory.INSTANCE,
                NoOpMetadataOperationsFactory.INSTANCE,
                URI.create("/FDB/FRL1"),
                false) {

            @Override
            public Expression visitSubscriptExpression(@Nonnull final RelationalParser.SubscriptExpressionContext ctx) {
                baseVisitorCalled.setTrue();
                return Expression.ofUnnamed(LiteralValue.ofScalar(42));
            }
        };
        final var delegatingVisitor = new DelegatingVisitor<>(baseVisitor);
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var parser = new RelationalParser(new CommonTokenStream(tokenSource));
        final var predicatedExpression = (RelationalParser.SubscriptExpressionContext)parser.expressionAtom();
        delegatingVisitor.visitSubscriptExpression(predicatedExpression);
        Assertions.assertThat(baseVisitorCalled.booleanValue()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("traversalStrings")
    void visitTraversalExpressionTest(String query) {
        final MutableBoolean baseVisitorCalled = new MutableBoolean(false);
        final var baseVisitor = new BaseVisitor(
                new MutablePlanGenerationContext(PreparedParams.empty(),
                        PlanHashable.PlanHashMode.VC0, query, query, 42),
                generateMetadata(),
                NoOpQueryFactory.INSTANCE,
                NoOpMetadataOperationsFactory.INSTANCE,
                URI.create("/FDB/FRL1"),
                false) {

            @Override
            public Object visitTraversalStrategy(final RelationalParser.TraversalStrategyContext ctx) {
                baseVisitorCalled.setTrue();
                if (query.equals("LEVEL")) {
                    return RecursiveUnionExpression.Traversal.LEVEL;
                } else {
                    return RecursiveUnionExpression.Traversal.PREORDER;
                }
            }
        };
        final var delegatingVisitor = new DelegatingVisitor<>(baseVisitor);
        final var tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final var parser = new RelationalParser(new CommonTokenStream(tokenSource));
        final var predicatedExpression = parser.traversalStrategy();
        delegatingVisitor.visitTraversalStrategy(predicatedExpression);
        Assertions.assertThat(baseVisitorCalled.booleanValue()).isTrue();
    }
}
