/*
 * PlanContext.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public final class PlanContext {

    // todo (yhatem) remove this if possible.
    private final RecordMetaData metaData;
    private final MetricCollector metricCollector;
    private final PlannerConfiguration plannerConfiguration;
    private final MetadataOperationsFactory metadataOperationsFactory;
    private final DdlQueryFactory ddlQueryFactory;
    private final URI dbUri;
    private final PreparedParams preparedStatementParameters;
    private final SchemaTemplate schemaTemplate;

    private final boolean isCaseSensitive;

    /**
     * Creates a new instance of {@link PlanContext} needed for generating plans.
     *
     * @param metaData                    The record store metadata.
     * @param metricCollector             The metricCollector instance bound to the ongoing transaction
     * @param schemaTemplate              The schema template.
     * @param plannerConfiguration        The planner configurations.
     * @param metadataOperationsFactory   The constant action factory used for DDL and metadata queries
     * @param dbUri                       The URI of the database.
     * @param ddlQueryFactory             The DDL factory.
     * @param preparedStatementParameters A list of prepared statement parameters.
     * @param isCaseSensitive             {@code True} if SQL identifiers should be treated as case-sensitive, otherwise
     *                                    {@code false}.
     **/
    private PlanContext(RecordMetaData metaData,
                        MetricCollector metricCollector,
                        SchemaTemplate schemaTemplate,
                        PlannerConfiguration plannerConfiguration,
                        MetadataOperationsFactory metadataOperationsFactory,
                        DdlQueryFactory ddlQueryFactory,
                        URI dbUri,
                        PreparedParams preparedStatementParameters,
                        boolean isCaseSensitive) {
        this.metaData = metaData;
        this.metricCollector = metricCollector;
        this.schemaTemplate = schemaTemplate;
        this.plannerConfiguration = plannerConfiguration;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.preparedStatementParameters = preparedStatementParameters;
        this.isCaseSensitive = isCaseSensitive;
    }

    public RecordMetaData getMetaData() {
        return metaData;
    }

    public MetricCollector getMetricsCollector() {
        return metricCollector;
    }

    public PlannerConfiguration getPlannerConfiguration() {
        return plannerConfiguration;
    }

    public Optional<Set<String>> getReadableIndexes() {
        return plannerConfiguration.getReadableIndexes();
    }

    public RecordQueryPlannerConfiguration getRecordQueryPlannerConfiguration() {
        return plannerConfiguration.getRecordQueryPlannerConfiguration();
    }

    public MetadataOperationsFactory getConstantActionFactory() {
        return metadataOperationsFactory;
    }

    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    public URI getDbUri() {
        return dbUri;
    }

    public PreparedParams getPreparedStatementParameters() {
        return preparedStatementParameters;
    }

    public SchemaTemplate getSchemaTemplate() {
        return schemaTemplate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private RecordMetaData metaData;

        private MetricCollector metricCollector;

        private PlannerConfiguration plannerConfiguration;

        private SchemaTemplate schemaTemplate;

        private MetadataOperationsFactory metadataOperationsFactory;

        private DdlQueryFactory ddlQueryFactory;

        private URI dbUri;

        private PreparedParams preparedStatementParameters;

        private boolean isCaseSensitive;

        private Builder() {
        }

        @VisibleForTesting
        public Builder withMetadata(RecordMetaData metadata) {
            this.metaData = metadata;
            return this;
        }

        public Builder withMetricsCollector(MetricCollector metricCollector) {
            this.metricCollector = metricCollector;
            return this;
        }

        public Builder withSchemaTemplate(SchemaTemplate schemaTemplate) {
            this.schemaTemplate = schemaTemplate;
            return this;
        }

        @VisibleForTesting
        public Builder withPlannerConfiguration(PlannerConfiguration plannerConfiguration) {
            this.plannerConfiguration = plannerConfiguration;
            return this;
        }

        private Builder isCaseSensitive(boolean isCaseSensitive) {
            this.isCaseSensitive = isCaseSensitive;
            return this;
        }

        @VisibleForTesting
        public Builder withConstantActionFactory(MetadataOperationsFactory metadataOperationsFactory) {
            this.metadataOperationsFactory = metadataOperationsFactory;
            return this;
        }

        public Builder withDdlQueryFactory(DdlQueryFactory ddlQueryFactory) {
            this.ddlQueryFactory = ddlQueryFactory;
            return this;
        }

        public Builder withDbUri(URI dbUri) {
            this.dbUri = dbUri;
            return this;
        }

        public Builder withPreparedParameters(PreparedParams parameters) {
            this.preparedStatementParameters = parameters;
            return this;
        }

        private static Optional<Set<String>> getReadableIndexes(RecordMetaData metaData,
                                                                RecordStoreState storeState) {
            // (yhatem) we should cache this somewhere, or embed it in the caching logic of the {@code FDBRecordStoreBase#createOrOpen}.
            if (storeState.allIndexesReadable()) {
                return Optional.empty();
            } else {
                final var universalIndexes = metaData.getUniversalIndexes();
                return Optional.of(metaData.getAllIndexes().stream().filter(storeState::isReadable).filter(index ->
                        !universalIndexes.contains(index)).map(com.apple.foundationdb.record.metadata.Index::getName).collect(Collectors.toUnmodifiableSet()));
            }
        }

        public Builder fromRecordStore(FDBRecordStoreBase<?> recordStore, final Options options) {
            return fromMetaDataAndState(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), options);
        }

        public Builder fromMetaDataAndState(RecordMetaData metaData,
                                            RecordStoreState recordStoreState,
                                            final Options options) {
            final var plannerConfig = recordStoreState.allIndexesReadable() ?
                    PlannerConfiguration.ofAllAvailableIndexes(options) :
                    PlannerConfiguration.of(getReadableIndexes(metaData, recordStoreState), options);
            return withPlannerConfiguration(plannerConfig)
                    .withMetadata(metaData)
                    .isCaseSensitive(options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS));
        }

        public Builder fromDatabase(AbstractDatabase database) {
            return withDdlQueryFactory(database.getDdlQueryFactory())
                    .withConstantActionFactory(database.getDdlFactory())
                    .withDbUri(database.getURI());
        }

        private void verify() throws RelationalException {
            Assert.notNull(metaData);
            Assert.notNull(schemaTemplate);
            Assert.notNull(plannerConfiguration);
            Assert.notNull(metadataOperationsFactory);
            Assert.notNull(ddlQueryFactory);
            Assert.notNull(dbUri);
            if (preparedStatementParameters == null) {
                preparedStatementParameters = PreparedParams.empty();
            }
        }

        public PlanContext build() throws RelationalException {
            verify();
            return new PlanContext(metaData, metricCollector, schemaTemplate, plannerConfiguration, metadataOperationsFactory,
                    ddlQueryFactory, dbUri, preparedStatementParameters, isCaseSensitive);
        }

        public static Builder create() {
            return new Builder();
        }

        @VisibleForTesting
        public static Builder unapply(PlanContext planContext) {
            return create().withConstantActionFactory(planContext.metadataOperationsFactory)
                    .withDbUri(planContext.dbUri)
                    .withMetadata(planContext.metaData)
                    .withMetricsCollector(planContext.metricCollector)
                    .withSchemaTemplate(planContext.schemaTemplate)
                    .withDdlQueryFactory(planContext.ddlQueryFactory)
                    .withPlannerConfiguration(planContext.plannerConfiguration)
                    .withPreparedParameters(planContext.preparedStatementParameters)
                    .isCaseSensitive(planContext.isCaseSensitive);
        }
    }
}
