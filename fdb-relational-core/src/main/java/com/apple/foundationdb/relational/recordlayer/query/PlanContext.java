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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.net.URI;

public final class PlanContext {
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final RecordStoreState storeState;
    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;
    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;
    @Nonnull
    private final URI dbUri;

    /**
     * Creates a new instance of {@link PlanContext} needed for generating plans.
     *
     * @param metaData              The record store metadata.
     * @param storeState            The record store state.
     * @param metadataOperationsFactory The constant action factory used for DDL and metadata queries
     * @param dbUri                 The URI of the database.
     **/
    private PlanContext(@Nonnull final RecordMetaData metaData,
                        @Nonnull final RecordStoreState storeState,
                        @Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                        @Nonnull final DdlQueryFactory ddlQueryFactory,
                        @Nonnull final URI dbUri) {
        this.metaData = metaData;
        this.storeState = storeState;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
    }

    @Nonnull
    public RecordMetaData getMetaData() {
        return metaData;
    }

    @Nonnull
    public RecordStoreState getStoreState() {
        return storeState;
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

    public static final class Builder {

        private RecordMetaData metaData;

        private RecordStoreState storeState;

        private MetadataOperationsFactory metadataOperationsFactory;

        private DdlQueryFactory ddlQueryFactory;

        private URI dbUri;

        private Builder() {
        }

        @Nonnull
        public Builder withMetadata(@Nonnull final RecordMetaData metadata) {
            this.metaData = metadata;
            return this;
        }

        @Nonnull
        public Builder withStoreState(@Nonnull final RecordStoreState storeState) {
            this.storeState = storeState;
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
        public Builder withDbUri(@Nonnull final URI dbUri) {
            this.dbUri = dbUri;
            return this;
        }

        @Nonnull
        public Builder fromRecordStore(@Nonnull final FDBRecordStore recordStore) {
            return withStoreState(recordStore.getRecordStoreState()).withMetadata(recordStore.getRecordMetaData());
        }

        @Nonnull
        public Builder fromDatabase(@Nonnull final AbstractDatabase database) {
            return withDdlQueryFactory(database.getDdlQueryFactory())
                    .withConstantActionFactory(database.getDdlFactory())
                    .withDbUri(database.getURI());
        }

        private void verify() throws RelationalException {
            Assert.notNull(metaData);
            Assert.notNull(storeState);
            Assert.notNull(metadataOperationsFactory);
            Assert.notNull(ddlQueryFactory);
            Assert.notNull(dbUri);
        }

        @Nonnull
        public PlanContext build() throws RelationalException {
            verify();
            return new PlanContext(metaData, storeState, metadataOperationsFactory, ddlQueryFactory, dbUri);
        }

        @Nonnull
        public static Builder create() {
            return new Builder();
        }

        public static Builder unapply(@Nonnull final PlanContext planContext) {
            return create().withConstantActionFactory(planContext.metadataOperationsFactory)
                    .withDbUri(planContext.dbUri)
                    .withMetadata(planContext.metaData)
                    .withDdlQueryFactory(planContext.ddlQueryFactory)
                    .withStoreState(planContext.storeState);
        }
    }
}
