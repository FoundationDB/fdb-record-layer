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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.QueryExecutor;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.ValueTuple;
import com.apple.foundationdb.relational.recordlayer.query.cache.PlanCache;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface QueryPlan extends Plan<RelationalResultSet>, Typed {

    /**
     * This represents a logical query plan that can be executed to produce a {@link java.sql.ResultSet}.
     */
    class LogicalQueryPlan implements QueryPlan {

        @Nonnull
        private final RelationalExpression relationalExpression;

        private final int limit;

        private final int offset;

        @Nonnull
        private final String query;

        private final boolean forExplain;

        @Nullable
        byte[] continuation;

        private LogicalQueryPlan(@Nonnull final RelationalExpression relationalExpression,
                                 @Nonnull final String query,
                                 boolean forExplain,
                                 int limit,
                                 int offset,
                                 @Nullable byte[] continuation) {
            this.relationalExpression = relationalExpression;
            this.query = query;
            this.forExplain = forExplain;
            this.continuation = continuation;
            this.limit = limit;
            this.offset = offset;
        }

        /**
         * Parses a query, generates an equivalent logical plan and calls the planner to generate an execution plan.
         *
         * @param planContext           the plan context
         * @return The execution plan of the query.
         * @throws RelationalException if something goes wrong.
         */
        @Nonnull
        @VisibleForTesting
        private RecordQueryPlan generatePhysicalPlan(@Nonnull final PlanContext planContext) throws RelationalException {
            PlanCache cache = planContext.getPlanCache();
            RecordQueryPlan plan = null;
            LogicalQuery logicalQuery = LogicalQuery.of(query, relationalExpression);
            if (cache != null) {
                //TODO(bfines) this is where we input the stripped literals
                plan = cache.getPlan(logicalQuery, planContext.getSchemaState());
            }
            if (plan == null) {
                final CascadesPlanner planner = createPlanner(planContext);
                try {
                    plan = planner.planGraph(
                            () -> GroupExpressionRef.of(relationalExpression),
                            Optional.empty(),
                            IndexQueryabilityFilter.TRUE,
                            ((LogicalSortExpression) relationalExpression).isReverse(), ParameterRelationshipGraph.empty());
                } catch (UncheckedRelationalException uve) {
                    throw uve.unwrap();
                }

                if (cache != null) {
                    cache.cacheEntry(logicalQuery, plan);
                }
            }
            return plan;
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext context) throws RelationalException {
            if (!(context.connection instanceof EmbeddedRelationalConnection)) {
                //this is required until TODO is resolved
                throw new RelationalException("Cannot execute a QueryPlan without an EmbeddedRelationalConnection", ErrorCode.INTERNAL_ERROR);
            }
            EmbeddedRelationalConnection conn = (EmbeddedRelationalConnection) context.connection;
            final String schemaName = conn.getSchema();
            try (RecordLayerSchema recordLayerSchema = conn.getRecordLayerDatabase().loadSchema(schemaName)) {
                final FDBRecordStore store = recordLayerSchema.loadStore();
                final var planContext = PlanContext.Builder.create().fromDatabase(conn.getRecordLayerDatabase()).fromRecordStore(store).build();
                RecordQueryPlan recordQueryPlan = generatePhysicalPlan(planContext);
                if (forExplain) {
                    return explainPhysicalPlan(recordQueryPlan);
                } else {
                    return executePhysicalPlan(recordQueryPlan, recordLayerSchema, conn);
                }
            }
        }

        @Nonnull
        private RelationalResultSet explainPhysicalPlan(@Nonnull final RecordQueryPlan physicalPlan) {
            List<String> explainComponents = new ArrayList<>();
            explainComponents.add(physicalPlan.toString());
            if (limit != ReadTransaction.ROW_LIMIT_UNLIMITED) {
                explainComponents.add(String.format("(limit=%d)", limit));
            }
            if (offset != 0) {
                explainComponents.add(String.format("(offset=%d)", offset));
            }
            Row printablePlan = new ValueTuple(String.join(" ", explainComponents));
            StructMetaData metaData = new RelationalStructMetaData(
                    FieldDescription.primitive("PLAN", Types.VARCHAR, DatabaseMetaData.columnNoNulls)
            );
            return new IteratorResultSet(metaData, Collections.singleton(printablePlan).iterator(), 0);
        }

        @Nonnull
        private RelationalResultSet executePhysicalPlan(@Nonnull final RecordQueryPlan physicalPlan,
                                                      @Nonnull final RecordLayerSchema recordLayerSchema,
                                                      @Nonnull final EmbeddedRelationalConnection connection) throws RelationalException {
            final Type innerType = relationalExpression.getResultType().getInnerType();
            Assert.notNull(innerType);
            Assert.that(innerType instanceof Type.Record, String.format("unexpected plan returning top-level result of type %s", innerType.getTypeCode()));
            final TypeRepository.Builder builder = TypeRepository.newBuilder();
            final Set<Type> usedTypes = UsedTypesProperty.evaluate(physicalPlan);
            usedTypes.forEach(builder::addTypeIfNeeded);
            final String[] fieldNames = Objects.requireNonNull(((Type.Record) innerType).getFields()).stream().sorted(Comparator.comparingInt(Type.Record.Field::getFieldIndex)).map(Type.Record.Field::getFieldName).collect(Collectors.toUnmodifiableList()).toArray(String[]::new);
            final QueryExecutor queryExecutor = new QueryExecutor(physicalPlan, fieldNames, EvaluationContext.forTypeRepository(builder.build()), recordLayerSchema, false /* get this information from the query plan */);
            Type type = queryExecutor.getQueryResultType();
            StructMetaData metaData = SqlTypeSupport.typeToMetaData(type);
            var executeProperties = ExecuteProperties.newBuilder().setSkip(offset).setReturnedRowLimit(limit).build();
            return new RecordLayerResultSet(metaData,
                    queryExecutor.execute(ContinuationImpl.fromBytes(continuation), executeProperties), connection);
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return relationalExpression.getResultType();
        }

        @Nonnull
        public LogicalQueryPlan forExplain() {
            return new LogicalQueryPlan(relationalExpression, query.stripLeading().substring(7), true, limit, offset, continuation);
        }

        @Nonnull
        public static LogicalQueryPlan of(@Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final String query,
                                          int limit,
                                          int offset) {
            return LogicalQueryPlan.of(relationalExpression, query, false, limit, offset);
        }

        @Nonnull
        public static LogicalQueryPlan of(@Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final String query,
                                          boolean forExplain,
                                          int limit,
                                          int offset) {
            return LogicalQueryPlan.of(relationalExpression, query, forExplain, limit, offset, null);
        }

        @Nonnull
        public static LogicalQueryPlan of(@Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final
                                          String query,
                                          boolean forExplain,
                                          int limit,
                                          int offset,
                                          @Nullable final byte[] continuation) {
            return new LogicalQueryPlan(relationalExpression, query, forExplain, limit, offset, continuation);
        }
    }

    private static CascadesPlanner createPlanner(PlanContext planContext) {
        CascadesPlanner planner = new CascadesPlanner(planContext.getMetaData(), planContext.getStoreState());
        RecordQueryPlannerConfiguration configuration = RecordQueryPlannerConfiguration.builder()
                .setIndexFetchMethod(IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK)
                .build();
        planner.setConfiguration(configuration);

        return planner;
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
