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
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
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
    private final PreparedParams preparedStatementParameters;
    @Nonnull
    private final SchemaTemplate schemaTemplate;

    private final int userVersion;

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
     * @param userVersion                 The user version bound to the opened record store.
     * @param isCaseSensitive             {@code True} if SQL identifiers should be treated as case-sensitive, otherwise
     *                                    {@code false}.
     **/
    private PlanContext(@Nonnull RecordMetaData metaData,
                        @Nonnull MetricCollector metricCollector,
                        @Nonnull SchemaTemplate schemaTemplate,
                        @Nonnull PlannerConfiguration plannerConfiguration,
                        @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                        @Nonnull DdlQueryFactory ddlQueryFactory,
                        @Nonnull URI dbUri,
                        @Nonnull PreparedParams preparedStatementParameters,
                        final int userVersion,
                        boolean isCaseSensitive) {
        this.metaData = metaData;
        this.metricCollector = metricCollector;
        this.schemaTemplate = schemaTemplate;
        this.plannerConfiguration = plannerConfiguration;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.preparedStatementParameters = preparedStatementParameters;
        this.userVersion = userVersion;
        this.isCaseSensitive = isCaseSensitive;
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
    public Optional<Set<String>> getReadableIndexes() {
        return plannerConfiguration.getReadableIndexes();
    }

    @Nonnull
    public RecordQueryPlannerConfiguration getRecordQueryPlannerConfiguration() {
        return plannerConfiguration.getRecordQueryPlannerConfiguration();
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
    public PreparedParams getPreparedStatementParameters() {
        return preparedStatementParameters;
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate() {
        return schemaTemplate;
    }

    public int getUserVersion() {
        return userVersion;
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
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

        private PreparedParams preparedStatementParameters;

        private boolean isCaseSensitive;

        private Builder() {
        }

        @Nonnull
        @VisibleForTesting
        public Builder withMetadata(@Nonnull RecordMetaData metadata) {
            this.metaData = metadata;
            return this;
        }

        @Nonnull
        public Builder withMetricsCollector(@Nonnull MetricCollector metricCollector) {
            this.metricCollector = metricCollector;
            return this;
        }

        @Nonnull
        public Builder withSchemaTemplate(@Nonnull SchemaTemplate schemaTemplate) {
            this.schemaTemplate = schemaTemplate;
            return this;
        }

        @Nonnull
        @VisibleForTesting
        public Builder withPlannerConfiguration(@Nonnull PlannerConfiguration plannerConfiguration) {
            this.plannerConfiguration = plannerConfiguration;
            return this;
        }

        @Nonnull
        @VisibleForTesting
        public Builder withUserVersion(int userVersion) {
            this.userVersion = userVersion;
            return this;
        }

        @Nonnull
        private Builder isCaseSensitive(boolean isCaseSensitive) {
            this.isCaseSensitive = isCaseSensitive;
            return this;
        }

        @Nonnull
        @VisibleForTesting
        public Builder withConstantActionFactory(@Nonnull MetadataOperationsFactory metadataOperationsFactory) {
            this.metadataOperationsFactory = metadataOperationsFactory;
            return this;
        }

        @Nonnull
        public Builder withDdlQueryFactory(@Nonnull DdlQueryFactory ddlQueryFactory) {
            this.ddlQueryFactory = ddlQueryFactory;
            return this;
        }

        @Nonnull
        public Builder withDbUri(@Nonnull URI dbUri) {
            this.dbUri = dbUri;
            return this;
        }

        @Nonnull
        public Builder withPreparedParameters(@Nonnull PreparedParams parameters) {
            this.preparedStatementParameters = parameters;
            return this;
        }

        @Nonnull
        private static Optional<Set<String>> getReadableIndexes(@Nonnull RecordMetaData metaData,
                                                                @Nonnull RecordStoreState storeState) {
            // (yhatem) we should cache this somewhere, or embed it in the caching logic of the {@code FDBRecordStoreBase#createOrOpen}.
            if (storeState.allIndexesReadable()) {
                return Optional.empty();
            } else {
                final var universalIndexes = metaData.getUniversalIndexes();
                return Optional.of(metaData.getAllIndexes().stream().filter(storeState::isReadable).filter(index ->
                        !universalIndexes.contains(index)).map(com.apple.foundationdb.record.metadata.Index::getName).collect(Collectors.toUnmodifiableSet()));
            }
        }

        @Nonnull
        public Builder fromRecordStore(@Nonnull FDBRecordStoreBase<?> recordStore, @Nonnull final Options options) {
            final var plannerConfig = recordStore.getRecordStoreState().allIndexesReadable() ?
                    PlannerConfiguration.ofAllAvailableIndexes(options) :
                    PlannerConfiguration.of(getReadableIndexes(recordStore.getRecordMetaData(), recordStore.getRecordStoreState()), options);
            return withPlannerConfiguration(plannerConfig)
                    .withMetadata(recordStore.getRecordMetaData())
                    .withUserVersion(recordStore.getRecordStoreState().getStoreHeader().getUserVersion())
                    .isCaseSensitive(options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS));
        }

        @Nonnull
        public Builder fromDatabase(@Nonnull AbstractDatabase database) {
            return withDdlQueryFactory(database.getDdlQueryFactory())
                    .withConstantActionFactory(database.getDdlFactory())
                    .withDbUri(database.getURI());
        }

        private void verify() {
            Assert.notNullUnchecked(metaData);
            Assert.notNullUnchecked(schemaTemplate);
            Assert.notNullUnchecked(plannerConfiguration);
            Assert.notNullUnchecked(metadataOperationsFactory);
            Assert.notNullUnchecked(ddlQueryFactory);
            Assert.notNullUnchecked(dbUri);
            if (preparedStatementParameters == null) {
                preparedStatementParameters = PreparedParams.empty();
            }
        }

        @Nonnull
        public PlanContext build() {
            verify();
            return new PlanContext(metaData, metricCollector, schemaTemplate, plannerConfiguration, metadataOperationsFactory,
                    ddlQueryFactory, dbUri, preparedStatementParameters, userVersion, isCaseSensitive);
        }

        @Nonnull
        public static Builder create() {
            return new Builder();
        }

        @VisibleForTesting
        @Nonnull
        public static Builder unapply(@Nonnull PlanContext planContext) {
            return create().withConstantActionFactory(planContext.metadataOperationsFactory)
                    .withDbUri(planContext.dbUri)
                    .withMetadata(planContext.metaData)
                    .withMetricsCollector(planContext.metricCollector)
                    .withSchemaTemplate(planContext.schemaTemplate)
                    .withDdlQueryFactory(planContext.ddlQueryFactory)
                    .withPlannerConfiguration(planContext.plannerConfiguration)
                    .withUserVersion(planContext.userVersion)
                    .withPreparedParameters(planContext.preparedStatementParameters)
                    .isCaseSensitive(planContext.isCaseSensitive);
        }
    }
}
