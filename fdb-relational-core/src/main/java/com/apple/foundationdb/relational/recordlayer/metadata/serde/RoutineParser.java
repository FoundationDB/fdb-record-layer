/*
 * RoutineParser.java
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

package com.apple.foundationdb.relational.recordlayer.metadata.serde;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.MutablePlanGenerationContext;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.QueryParser;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface RoutineParser {

    @Nonnull
    UserDefinedFunction parseFunction(@Nonnull String routineString, boolean isCaseSensitive);

    @Nonnull
    LogicalOperator parseView(@Nonnull String viewName, @Nonnull String viewDefinition, boolean isCaseSensitive);

    class DefaultSqlFunctionParser implements RoutineParser {

        @Nonnull
        private final RecordLayerSchemaTemplate metaData;

        private DefaultSqlFunctionParser(@Nonnull final RecordLayerSchemaTemplate metaData) {
            this.metaData = metaData;
        }

        @Nonnull
        @Override
        public CompiledSqlFunction parseFunction(@Nonnull final String routineString, boolean isCaseSensitive) {
            return (CompiledSqlFunction)parse(routineString, null, PreparedParams.empty(), QueryParser::parseFunction,
                    BaseVisitor::visitSqlInvokedFunction, isCaseSensitive);
        }

        @Nonnull
        @Override
        public LogicalOperator parseView(@Nonnull final String viewName,
                                         @Nonnull final String viewDefinition,
                                         boolean isCaseSensitive) {
            return parse(viewDefinition, viewName, PreparedParams.empty(), QueryParser::parseView,
                    (v, p) -> v.getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                            Assert.castUnchecked(v.visit(p), LogicalOperator.class)), isCaseSensitive);
        }

        @Nonnull
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private <P, T> T parse(@Nonnull final String query,
                               @Nullable String scope,
                               @Nonnull final PreparedParams preparedParams,
                               @Nonnull final Function<String, P> parse,
                               @Nonnull final BiFunction<BaseVisitor, P, T> visit,
                               boolean isCaseSensitive) {
            final var parsed = parse.apply(query);
//            final var astNormalizer = AstNormalizer.normalizeAst(
//                    metaData,
//                    parsed,
//                    preparedParams,
//                    userVersion,
//                    PlannerConfiguration.of(),
//                    isCaseSensitive,
//                    PlanHashable.PlanHashMode.VC0,
//                    query);
            final var planGenerationContext = new MutablePlanGenerationContext(preparedParams,
                    PlanHashable.PlanHashMode.VC0, query, query, 0);
            if (scope != null) {
                planGenerationContext.getLiteralsBuilder().setScope(scope);
            }

            final var visitor = new BaseVisitor(planGenerationContext, metaData, new NoOpQueryFactory(),
                    NoOpMetadataOperationsFactory.INSTANCE, URI.create(""), null, isCaseSensitive);
            return visit.apply(visitor, parsed);
        }
    }

    @Nonnull
    static DefaultSqlFunctionParser sqlFunctionParser(@Nonnull final RecordLayerSchemaTemplate metaData) {
        return new DefaultSqlFunctionParser(metaData);
    }
}
