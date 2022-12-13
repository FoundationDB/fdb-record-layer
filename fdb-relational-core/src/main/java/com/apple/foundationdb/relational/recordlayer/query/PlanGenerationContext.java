/*
 * ParserContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Context keeping state related to plan generation.
 */
public class PlanGenerationContext {

    @Nullable
    private AbstractContext context;

    @Nonnull
    private final MetadataOperationsFactory metadataFactory;

    PlanGenerationContext(@Nonnull MetadataOperationsFactory metadataFactory) {
        this.context = null;
        this.metadataFactory = metadataFactory;
    }

    @Nonnull
    public DMLContext pushDmlContext(@Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
        this.context = new DMLContext(context, recordLayerSchemaTemplate);
        return (DMLContext)context;
    }

    @Nonnull
    public DDLContext pushDdlContext() {
        return pushDdlContext(RecordLayerSchemaTemplate.newBuilder());
    }

    @Nonnull
    public DDLContext pushDdlContext(@Nonnull final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder) {
        this.context = new DDLContext(context,  schemaTemplateBuilder, metadataFactory);
        return (DDLContext) context;
    }

    @Nullable
    public AbstractContext pop() {
        Assert.notNullUnchecked(context, "attempt to remove non-existing context");
        try {
            return context;
        } finally {
            context = context.parent;
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public DDLContext asDdl() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, expected '%s', however current context is not initialized!", DDLContext.class.getName()));
        if (context instanceof DDLContext) {
            return (DDLContext)context;
        }
        Assert.failUnchecked(String.format("plan generation context mismatch, expected '%s', got '%s'.", context.getClass().getName(), DDLContext.class.getName()));
        return null; // make compiler happy.
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public DMLContext asDml() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, expected '%s', however current context is not initialized!", DMLContext.class.getName()));
        if (context instanceof DMLContext) {
            return (DMLContext)context;
        }
        Assert.failUnchecked(String.format("plan generation context mismatch, expected '%s', got '%s'.", context.getClass().getName(), DMLContext.class.getName()));
        return null; // make compiler happy.
    }

    public static class Builder {
        private MetadataOperationsFactory metadataFactory;

        private Builder() {
            this.metadataFactory = NoOpMetadataOperationsFactory.INSTANCE;
        }

        public Builder setMetadataFactory(@Nonnull final MetadataOperationsFactory metadataFactory) {
            this.metadataFactory = metadataFactory;
            return this;
        }

        public PlanGenerationContext build() {
            return new PlanGenerationContext(metadataFactory);
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod") // intentional.
    public abstract static class AbstractContext {

        private final AbstractContext parent;

        protected AbstractContext() {
            this(null);
        }

        protected AbstractContext(@Nullable AbstractContext parent) {
            this.parent = parent;
        }

        @Nullable
        public AbstractContext getParent() {
            return parent;
        }
    }

    public static class DMLContext extends AbstractContext {
        private int limit;

        private int offset;

        @Nonnull
        private final RecordLayerSchemaTemplate recordLayerSchemaTemplate;

        public DMLContext(@Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
            this(null, recordLayerSchemaTemplate);
        }

        public DMLContext(@Nullable AbstractContext parent,
                          @Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
            super(parent);
            this.recordLayerSchemaTemplate = recordLayerSchemaTemplate;
            this.limit = ReadTransaction.ROW_LIMIT_UNLIMITED;
            this.offset = 0;
        }

        @Nonnull
        public RecordLayerSchemaTemplate getRecordLayerSchemaTemplate() {
            return recordLayerSchemaTemplate;
        }

        public void setLimit(@Nonnull Integer limit) {
            this.limit = limit;
        }

        public void setOffset(@Nonnull Integer offset) {
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public Integer getOffset() {
            return offset;
        }

        @Nonnull
        public Set<String> getScannableRecordTypeNames() {
            return recordLayerSchemaTemplate.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()); // refactor and cache.
        }

        @Nonnull
        public Set<String> getIndexNames() {
            return new HashSet<>(recordLayerSchemaTemplate.getIndexes().values()); // refactor and cache.
        }
    }

    public static class DDLContext extends AbstractContext {

        @Nonnull
        private final RecordLayerSchemaTemplate.Builder builder;
        private MetadataOperationsFactory metadataOperationsFactory;

        private DDLContext(@Nullable AbstractContext parent, @Nonnull final RecordLayerSchemaTemplate.Builder builder,
                           @Nonnull final MetadataOperationsFactory metadataOperationsFactory) {
            super(parent);
            this.builder = builder;
            this.metadataOperationsFactory = metadataOperationsFactory;
        }

        @Nonnull
        public RecordLayerSchemaTemplate.Builder getMetadataBuilder() {
            return builder;
        }

        @Nonnull
        public MetadataOperationsFactory getMetadataOperationsFactory() {
            return metadataOperationsFactory;
        }
    }
}
