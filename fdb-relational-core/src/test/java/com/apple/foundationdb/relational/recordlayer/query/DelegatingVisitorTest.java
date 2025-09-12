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
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.apple.foundationdb.relational.recordlayer.query.visitors.DelegatingVisitor;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.net.URI;

public class DelegatingVisitorTest {

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
    void visitPredicatedExpressionTest() throws Exception {
        final var baseVisitor = new BaseVisitor(
                new MutablePlanGenerationContext(PreparedParams.empty(),
                        PlanHashable.PlanHashMode.VC0, "X BETWEEN 32 AND 43", "X BETWEEN 32 AND 43", 42),
                generateMetadata(),
                NoOpQueryFactory.INSTANCE,
                NoOpMetadataOperationsFactory.INSTANCE,
                URI.create("/FDB/FRL1"),
                false);
        final var delegatingVisitor = new DelegatingVisitor<>(baseVisitor);
        final var parsed = QueryParser.parse("X BETWEEN 32 AND 43").getRootContext();
        //delegatingVisitor.visitPredicatedExpression((RelationalParser.PredicatedExpressionContext)parsed);
        System.out.println("hello");
    }

}
