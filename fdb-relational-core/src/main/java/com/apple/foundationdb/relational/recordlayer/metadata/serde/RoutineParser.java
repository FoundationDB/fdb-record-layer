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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.MutablePlanGenerationContext;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.QueryParser;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompilableSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;

import javax.annotation.Nonnull;
import java.net.URI;

public interface RoutineParser {

    @Nonnull
    UserDefinedFunction parse(@Nonnull String routineString, boolean isCaseSensitive);

    @Nonnull
    UserDefinedFunction parseTemporaryFunction(@Nonnull String functionName, @Nonnull String routineString,
                                                                                                 @Nonnull PreparedParams preparedParams, boolean isCaseSensitive);

    class DefaultSqlFunctionParser implements RoutineParser {

        @Nonnull
        private final RecordLayerSchemaTemplate metaData;

        private DefaultSqlFunctionParser(@Nonnull final RecordLayerSchemaTemplate metaData) {
            this.metaData = metaData;
        }

        @Nonnull
        @Override
        public CompilableSqlFunction parse(@Nonnull final String routineString, boolean isCaseSensitive) {
            final RelationalParser.SqlInvokedFunctionContext parsed;
            try {
                parsed = QueryParser.parseFunction(routineString);
            } catch (RelationalException e) {
                throw e.toUncheckedWrappedException();
            }
            final var planGenerationContext = new MutablePlanGenerationContext(PreparedParams.empty(),
                    PlanHashable.PlanHashMode.VC0, routineString, routineString, 0);
            final var visitor = new BaseVisitor(planGenerationContext, metaData, new NoOpQueryFactory(),
                    NoOpMetadataOperationsFactory.INSTANCE, URI.create(""), isCaseSensitive);
            return (CompilableSqlFunction)visitor.visitSqlInvokedFunction(parsed);
        }

        @Nonnull
        @Override
        public CompilableSqlFunction parseTemporaryFunction(@Nonnull final String functionName,
                                                            @Nonnull final String routineString,
                                                            @Nonnull final PreparedParams preparedParams,
                                                            boolean isCaseSensitive) {
            final RelationalParser.TempSqlInvokedFunctionContext parsed;
            try {
                parsed = QueryParser.parseTemporaryFunction(routineString);
            } catch (RelationalException e) {
                throw e.toUncheckedWrappedException();
            }
            final var planGenerationContext = new MutablePlanGenerationContext(preparedParams,
                    PlanHashable.PlanHashMode.VC0, routineString, routineString, 0);
            planGenerationContext.getLiteralsBuilder().setScope(functionName);
            final var visitor = new BaseVisitor(planGenerationContext, metaData, new NoOpQueryFactory(),
                    NoOpMetadataOperationsFactory.INSTANCE, URI.create(""), isCaseSensitive);
            return visitor.visitTempSqlInvokedFunction(parsed);
        }
    }

    @Nonnull
    static DefaultSqlFunctionParser sqlFunctionParser(@Nonnull final RecordLayerSchemaTemplate metaData) {
        return new DefaultSqlFunctionParser(metaData);
    }
}
