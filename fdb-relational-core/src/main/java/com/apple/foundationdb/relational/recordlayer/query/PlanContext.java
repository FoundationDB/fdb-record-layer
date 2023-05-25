/*
 * PlanContext.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class PlanContext {

    // todo (yhatem) remove this if possible.
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final MetricCollector metricCollector;
    @Nonnull
    private final PlannerConfiguration plannerConfiguration;
    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;
    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;
    @Nonnull
    private final URI dbUri;
    @Nonnull
    private final PreparedStatementParameters preparedStatementParameters;

    @Nonnull
    private final SchemaTemplate schemaTemplate;

    private final int userVersion;

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
     * @param userVersion                 The user version bound to the opened record store.
     **/
    private PlanContext(@Nonnull final RecordMetaData metaData,
                        @Nonnull final MetricCollector metricCollector,
                        @Nonnull final SchemaTemplate schemaTemplate,
                        @Nonnull final PlannerConfiguration plannerConfiguration,
                        @Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                        @Nonnull final DdlQueryFactory ddlQueryFactory,
                        @Nonnull final URI dbUri,
                        @Nonnull final PreparedStatementParameters preparedStatementParameters,
                        final int userVersion) {
        this.metaData = metaData;
        this.metricCollector = metricCollector;
        this.schemaTemplate = schemaTemplate;
        this.plannerConfiguration = plannerConfiguration;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.preparedStatementParameters = preparedStatementParameters;
        this.userVersion = userVersion;
    }

    @Nonnull
    public RecordMetaData getMetaData() {
        return metaData;
    }

    @Nonnull
    public MetricCollector getMetricsCollector() {
        return metricCollector;
    }

    @Nonnull
    public PlannerConfiguration getPlannerConfiguration() {
        return plannerConfiguration;
    }

    @Nonnull
    public MetadataOperationsFactory getConstantActionFactory() {
        return metadataOperationsFactory;
    }

    @Nonnull
    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    @Nonnull
    public URI getDbUri() {
        return dbUri;
    }

    @Nonnull
    public PreparedStatementParameters getPreparedStatementParameters() {
        return preparedStatementParameters;
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate() {
        return schemaTemplate;
    }

    public int getUserVersion() {
        return userVersion;
    }

    public static final class Builder {

        private RecordMetaData metaData;

        private MetricCollector metricCollector;

        private PlannerConfiguration plannerConfiguration;

        private int userVersion;

        private SchemaTemplate schemaTemplate;

        private MetadataOperationsFactory metadataOperationsFactory;

        private DdlQueryFactory ddlQueryFactory;

        private URI dbUri;
        @Nullable
        private RelationalPlanCache planCache;

        private PreparedStatementParameters preparedStatementParameters;

        private Builder() {
        }

        @Nonnull
        public Builder withMetadata(@Nonnull final RecordMetaData metadata) {
            this.metaData = metadata;
            return this;
        }

        @Nonnull
        public Builder withMetricsCollector(@Nonnull final MetricCollector metricCollector) {
            this.metricCollector = metricCollector;
            return this;
        }

        @Nonnull
        public Builder withSchemaTemplate(@Nonnull final SchemaTemplate schemaTemplate) {
            this.schemaTemplate = schemaTemplate;
            return this;
        }

        @Nonnull
        public Builder withPlannerConfiguration(@Nonnull final PlannerConfiguration plannerConfiguration) {
            this.plannerConfiguration = plannerConfiguration;
            return this;
        }

        @Nonnull
        public Builder withUserVersion(final int userVersion) {
            this.userVersion = userVersion;
            return this;
        }

        @Nonnull
        public Builder withConstantActionFactory(@Nonnull final MetadataOperationsFactory metadataOperationsFactory) {
            this.metadataOperationsFactory = metadataOperationsFactory;
            return this;
        }

        @Nonnull
        public Builder withDdlQueryFactory(@Nonnull final DdlQueryFactory ddlQueryFactory) {
            this.ddlQueryFactory = ddlQueryFactory;
            return this;
        }

        @Nonnull
        public Builder withPlanCache(@Nullable final RelationalPlanCache planCache) {
            this.planCache = planCache;
            return this;
        }

        @Nonnull
        public Builder withDbUri(@Nonnull final URI dbUri) {
            this.dbUri = dbUri;
            return this;
        }

        @Nonnull
        public Builder withPreparedParameters(@Nonnull final PreparedStatementParameters parameters) {
            this.preparedStatementParameters = parameters;
            return this;
        }

        @Nonnull
        public Builder fromRecordStore(@Nonnull final FDBRecordStoreBase<Message> recordStore) {
            final var plannerConfig = recordStore.getRecordStoreState().allIndexesReadable() ?
                    PlannerConfiguration.ofAllAvailableIndexes() :
                    PlannerConfiguration.from(getReadableIndexes(recordStore.getRecordMetaData(), recordStore.getRecordStoreState()));
            return withPlannerConfiguration(plannerConfig)
                    .withMetadata(recordStore.getRecordMetaData())
                    .withUserVersion(recordStore.getRecordStoreState().getStoreHeader().getUserVersion());
        }

        @Nonnull
        private static Optional<Set<String>> getReadableIndexes(@Nonnull final RecordMetaData metaData,
                                                                 @Nonnull final RecordStoreState storeState) {
            // (yhatem) we should cache this somewhere, or embed it in the caching logic of the {@code FDBRecordStoreBase#createOrOpen}.
            if (storeState.allIndexesReadable()) {
                return Optional.empty();
            } else {
                final var universalIndexes = metaData.getUniversalIndexes();
                return Optional.of(metaData.getAllIndexes().stream().filter(storeState::isReadable).filter(index -> !universalIndexes.contains(index)).map(com.apple.foundationdb.record.metadata.Index::getName).collect(Collectors.toUnmodifiableSet()));
            }
        }

        @Nonnull
        public Builder fromDatabase(@Nonnull final AbstractDatabase database) {
            return withDdlQueryFactory(database.getDdlQueryFactory())
                    .withConstantActionFactory(database.getDdlFactory())
                    .withPlanCache(database.getPlanCache())
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
                preparedStatementParameters = PreparedStatementParameters.empty();
            }
        }

        @Nonnull
        public PlanContext build() throws RelationalException {
            verify();
            return new PlanContext(metaData, metricCollector, schemaTemplate, plannerConfiguration, metadataOperationsFactory, ddlQueryFactory, dbUri, preparedStatementParameters, userVersion);
        }

        @Nonnull
        public static Builder create() {
            return new Builder();
        }

        public static Builder unapply(@Nonnull final PlanContext planContext) {
            return create().withConstantActionFactory(planContext.metadataOperationsFactory)
                    .withDbUri(planContext.dbUri)
                    .withMetadata(planContext.metaData)
                    .withMetricsCollector(planContext.metricCollector)
                    .withSchemaTemplate(planContext.schemaTemplate)
                    .withDdlQueryFactory(planContext.ddlQueryFactory)
                    .withPlannerConfiguration(planContext.plannerConfiguration)
                    .withUserVersion(planContext.userVersion);
        }
    }
}
