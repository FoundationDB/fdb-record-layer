/*
 * QueryPlan.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.QueryExecutor;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface QueryPlan extends Plan<RelationalResultSet>, Typed {

    class QpQueryplan implements QueryPlan {

        @Nonnull
        private final RelationalExpression relationalExpression;

        @Nonnull
        private final String query;

        @Nullable
        private RecordQueryPlan recordQueryPlan;

        @Nullable
        byte[] continuation;

        private QpQueryplan(@Nonnull final RelationalExpression relationalExpression, @Nonnull final String query, @Nullable byte[] continuation) {
            this.relationalExpression = relationalExpression;
            this.query = query;
            this.recordQueryPlan = null;
            this.continuation = continuation;
        }

        /**
         * Parses a query, generates an equivalent logical plan and calls the planner to generate an execution plan.
         *
         * @param query                 The query string.
         * @param planContext           the plan context
         * @return The execution plan of the query.
         * @throws RelationalException if something goes wrong.
         */
        @Nonnull
        @VisibleForTesting
        private static RecordQueryPlan generatePhysicalPlan(@Nonnull final String query, @Nonnull final PlanContext planContext) throws RelationalException {
            final CascadesPlanner planner = new CascadesPlanner(planContext.getMetaData(), planContext.getStoreState());
            final Collection<String> recordTypeNames = new LinkedHashSet<>();
            // need to do this step, so we can populate the record type names.
            final var planContextWithPostProcessing = PlanContext.Builder.unapply(planContext).withPostProcessor(astWalker -> recordTypeNames.addAll(astWalker.getFilteredRecords())).build();
            Plan.generate(query, planContextWithPostProcessing);
            try {
                return planner.planGraph(
                        () -> {
                            final RelationalExpression relationalExpression;
                            try {
                                final Plan<?> plan = Plan.generate(query, planContextWithPostProcessing);
                                Assert.that(plan instanceof QpQueryplan);
                                relationalExpression = ((QpQueryplan) plan).relationalExpression;
                            } catch (RelationalException e) {
                                throw e.toUncheckedWrappedException();
                            }
                            final Quantifier qun = Quantifier.forEach(GroupExpressionRef.of(relationalExpression));
                            return GroupExpressionRef.of(new LogicalSortExpression(null, false, qun));
                        },
                        Optional.ofNullable(recordTypeNames.isEmpty() ? null : recordTypeNames),
                        Optional.empty(),
                        IndexQueryabilityFilter.TRUE,
                        false, ParameterRelationshipGraph.empty());
            } catch (UncheckedRelationalException uve) {
                throw uve.unwrap();
            }
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext context) throws RelationalException {
            if (!(context.connection instanceof EmbeddedRelationalConnection)) {
                //this is required until TODO is resolved
                throw new RelationalException("Cannot execute a QueryPlan without an EmbeddedRelationalConnection", ErrorCode.INTERNAL_ERROR);
            }
            EmbeddedRelationalConnection conn = (EmbeddedRelationalConnection) context.connection;
            final String schemaName = conn.getSchema();
            final Type innerType = relationalExpression.getResultType().getInnerType();
            Assert.notNull(innerType);
            Assert.that(innerType instanceof Type.Record, String.format("unexpected plan returning top-level result of type %s", innerType.getTypeCode()));
            final TypeRepository.Builder builder = TypeRepository.newBuilder();

            try (RecordLayerSchema recordLayerSchema = conn.getRecordLayerDatabase().loadSchema(schemaName)) {
                final FDBRecordStore store = recordLayerSchema.loadStore();
                final var planContext = PlanContext.Builder.create().fromDatabase(conn.getRecordLayerDatabase()).fromRecordStore(store).build();
                recordQueryPlan = generatePhysicalPlan(query, planContext);
                Assert.notNull(recordQueryPlan);
                final Set<Type> usedTypes = UsedTypesProperty.evaluate(recordQueryPlan);
                usedTypes.forEach(builder::addTypeIfNeeded);
                final String[] fieldNames = Objects.requireNonNull(((Type.Record) innerType).getFields()).stream().sorted(Comparator.comparingInt(Type.Record.Field::getFieldIndex)).map(Type.Record.Field::getFieldName).collect(Collectors.toUnmodifiableList()).toArray(String[]::new);
                final QueryExecutor queryExecutor = new QueryExecutor(recordQueryPlan, fieldNames, EvaluationContext.forTypeRepository(builder.build()), recordLayerSchema, false /* get this information from the query plan */);
                return new RecordLayerResultSet(queryExecutor.getFieldNames(),
                        queryExecutor.execute(ContinuationImpl.fromBytes(continuation)),
                        conn);
            }
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return relationalExpression.getResultType();
        }

        @Nonnull
        public static QpQueryplan of(@Nonnull final RelationalExpression relationalExpression, @Nonnull final String query) {
            return new QpQueryplan(relationalExpression, query, null);
        }

        @Nonnull
        public static QpQueryplan of(@Nonnull final RelationalExpression relationalExpression, @Nonnull final String query, @Nonnull final byte[] continuation) {
            return new QpQueryplan(relationalExpression, query, continuation);
        }

    }

    class MetadataQueryPlan implements QueryPlan {

        @Nonnull
        private final CheckedFunctional<Transaction, RelationalResultSet> query;

        @Nonnull
        private final Type rowType;

        private interface CheckedFunctional<T, R> {
            R apply(T t) throws RelationalException;
        }

        private MetadataQueryPlan(@Nonnull final CheckedFunctional<Transaction, RelationalResultSet> query, @Nonnull final Type rowType) {
            this.query = query;
            this.rowType = rowType;
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext context) throws RelationalException {
            return query.apply(context.transaction);
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return rowType;
        }

        public static MetadataQueryPlan of(DdlQuery ddlQuery) {
            return new MetadataQueryPlan(ddlQuery::executeAction, ddlQuery.getResultSetMetadata());
        }
    }
}
