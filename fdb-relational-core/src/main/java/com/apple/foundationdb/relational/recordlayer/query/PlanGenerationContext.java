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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Context keeping state related to plan generation.
 */
public class PlanGenerationContext implements QueryExecutionParameters {

    @Nullable
    private AbstractContext context;

    @Nonnull
    private final MetadataOperationsFactory metadataFactory;

    @Nonnull
    private final PreparedStatementParameters preparedStatementParameters;

    @Nonnull
    private final MetricCollector metricCollector;

    @Nonnull
    private final LiteralsBuilder literals;

    @Nonnull
    private final List<ConstantObjectValue> constantObjectValues;

    private boolean forExplain;

    @Nullable
    private byte[] continuation;

    private boolean shouldProcessLiteral;

    private int parameterHash;

    PlanGenerationContext(@Nonnull MetadataOperationsFactory metadataFactory, @Nonnull PreparedStatementParameters preparedStatementParameters,
                          @Nonnull MetricCollector metricCollector) {
        this.context = null;
        this.metadataFactory = metadataFactory;
        this.preparedStatementParameters = preparedStatementParameters;
        this.metricCollector = metricCollector;
        this.literals = LiteralsBuilder.newBuilder();
        this.constantObjectValues = new LinkedList<>();
        this.forExplain = false;
        this.setContinuation(null);
        this.shouldProcessLiteral = true;
    }

    public int startArrayLiteral() {
        return literals.startArrayLiteral();
    }

    public void finishArrayLiteral() {
        literals.finishArrayLiteral();
    }

    public int addStrippedLiteral(@Nullable final Object literal) {
        return literals.addLiteral(literal);
    }

    public void addLiteralReference(@Nonnull final ConstantObjectValue constantObjectValue) {
        if (!literals.isAddingArrayLiteral()) {
            constantObjectValues.add(constantObjectValue);
        }
    }

    @Nonnull
    public List<Object> getLiterals() {
        return literals.getLiterals();
    }

    @Nonnull
    public DQLContext pushDqlContext(@Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
        this.context = new DQLContext(context, recordLayerSchemaTemplate);
        return (DQLContext) context;
    }

    @Nonnull
    public DMLContext pushDmlContext() {
        this.context = new DMLContext(context);
        return (DMLContext) context;
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
            return (DDLContext) context;
        }
        Assert.failUnchecked(String.format("plan generation context mismatch, expected '%s', got '%s'.", DDLContext.class.getName(), context.getClass().getName()));
        return null; // make compiler happy.
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public DQLContext asDql() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, expected '%s', however current context is not initialized!", DQLContext.class.getName()));
        if (context instanceof DQLContext) {
            return (DQLContext) context;
        }
        Assert.failUnchecked(String.format("plan generation context mismatch, expected '%s', got '%s'.", DQLContext.class.getName(), context.getClass().getName()));
        return null; // make compiler happy.
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public DMLContext asDml() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, expected '%s', however current context is not initialized!", DMLContext.class.getName()));
        if (context instanceof DMLContext) {
            return (DMLContext) context;
        }
        Assert.failUnchecked(String.format("plan generation context mismatch, expected '%s', got '%s'.", DMLContext.class.getName(), context.getClass().getName()));
        return null; // make compiler happy.
    }

    public boolean isDml() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, examining whether context is '%s', however current context is not initialized!", DMLContext.class.getName()));
        return context.getType() == AbstractContext.TYPE.DML;
    }

    public boolean isDdl() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, examining whether context is '%s', however current context is not initialized!", DDLContext.class.getName()));
        return context.getType() == AbstractContext.TYPE.DDL;
    }

    public boolean hasDdlAncestor() {
        var runner = context;
        while (true) {
            if (runner == null) {
                return false;
            }
            if (runner.type == AbstractContext.TYPE.DDL) {
                return true;
            }
            runner = runner.parent;
        }
    }

    public boolean isDql() {
        Assert.notNullUnchecked(context, String.format("plan generation context mismatch, examining whether context is '%s', however current context is not initialized!", DQLContext.class.getName()));
        return context.getType() == AbstractContext.TYPE.DQL;
    }

    @Nonnull
    @Override
    public EvaluationContext getEvaluationContext(@Nonnull final TypeRepository typeRepository) {
        if (literals.isEmpty()) {
            return EvaluationContext.forTypeRepository(typeRepository);
        }
        final var builder = EvaluationContext.newBuilder();
        builder.setConstant(Quantifier.constant(), getLiterals());
        return builder.build(typeRepository);
    }

    @Nonnull
    @Override
    public ExecuteProperties getExecutionProperties() {
        final var builder = ExecuteProperties.newBuilder();
        if (context != null) {
            context.setExecuteProperties(builder);
        }
        return builder.build();
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP", justification = "Intentional")
    @Nullable
    @Override
    public byte[] getContinuation() {
        return continuation;
    }

    @Override
    public int getParameterHash() {
        return parameterHash;
    }

    public void setParameterHash(int parameterHash) {
        this.parameterHash = parameterHash;
    }

    @Nonnull
    @Override
    public PreparedStatementParameters getPreparedStatementParameters() {
        return preparedStatementParameters;
    }

    @Nonnull
    public MetricCollector getMetricsCollector() {
        return metricCollector;
    }

    @Override
    public boolean isForExplain() {
        return forExplain;
    }

    @Nonnull
    public QueryPlanConstraint getLiteralReferencesConstraint() {
        return QueryPlanConstraint.ofPredicates(constantObjectValues.stream()
                .map(parameter -> new ValuePredicate(OfTypeValue.from(parameter),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)))
                .collect(Collectors.toList()));
    }

    public void setForExplain(boolean forExplain) {
        this.forExplain = forExplain;
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentional")
    public void setContinuation(@Nullable byte[] continuation) {
        this.continuation = continuation;
    }

    public boolean shouldProcessLiteral() {
        return shouldProcessLiteral && !hasDdlAncestor();
    }

    private void setShouldProcessLiteral(boolean shouldProcessLiteral) {
        this.shouldProcessLiteral = shouldProcessLiteral;
    }

    /**
     * Runs a closure without literal processing, i.e. without translating a literal found in the
     * AST into a {@link ConstantObjectValue}. This is necessary in cases where the literal is found
     * in a context that does not contribute to the logical plan such as {@code limit} and {@code continuation}.
     * @param supplier The closure to run with literal processing disabled.
     * @return The result of the closure
     * @param <T> The type of the result of the closure.
     */
    @Nullable
    public <T> T withDisabledLiteralProcessing(@Nonnull final Supplier<T> supplier) {
        setShouldProcessLiteral(false);
        @Nullable final T result = supplier.get();
        setShouldProcessLiteral(true);
        return result;
    }

    public static final class Builder {
        private MetadataOperationsFactory metadataFactory;
        private PreparedStatementParameters preparedStatementParameters;
        private MetricCollector metricCollector;

        private Builder() {
            this.metadataFactory = NoOpMetadataOperationsFactory.INSTANCE;
        }

        public Builder setMetadataFactory(@Nonnull final MetadataOperationsFactory metadataFactory) {
            this.metadataFactory = metadataFactory;
            return this;
        }

        public Builder setPreparedStatementParameters(@Nonnull final PreparedStatementParameters preparedStatementParameters) {
            this.preparedStatementParameters = preparedStatementParameters;
            return this;
        }

        public Builder setMetricsCollector(@Nonnull final MetricCollector metricCollector) {
            this.metricCollector = metricCollector;
            return this;
        }

        public PlanGenerationContext build() {
            return new PlanGenerationContext(metadataFactory, preparedStatementParameters, metricCollector);
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod") // intentional.
    public abstract static class AbstractContext {

        @Nonnull
        enum TYPE {
            DDL,
            DML,
            DQL
        }

        @Nullable
        private final AbstractContext parent;

        @Nonnull
        private final TYPE type;

        protected AbstractContext(@Nonnull final TYPE type) {
            this(null, type);
        }

        protected AbstractContext(@Nullable AbstractContext parent, @Nonnull final TYPE type) {
            this.parent = parent;
            this.type = type;
        }

        @Nullable
        public AbstractContext getParent() {
            return parent;
        }

        @Nonnull
        TYPE getType() {
            return type;
        }

        abstract void setExecuteProperties(@Nonnull ExecuteProperties.Builder builder);
    }

    public static class DQLContext extends AbstractContext {

        private int limit;

        private int offset;

        @Nonnull
        private final RecordLayerSchemaTemplate recordLayerSchemaTemplate;

        public DQLContext(@Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
            this(null, recordLayerSchemaTemplate);
        }

        public DQLContext(@Nullable AbstractContext parent,
                          @Nonnull final RecordLayerSchemaTemplate recordLayerSchemaTemplate) {
            super(parent, TYPE.DQL);
            this.recordLayerSchemaTemplate = recordLayerSchemaTemplate;
            this.limit = ReadTransaction.ROW_LIMIT_UNLIMITED;
            this.offset = 0;
        }

        @Nonnull
        public RecordLayerSchemaTemplate getRecordLayerSchemaTemplate() {
            return recordLayerSchemaTemplate;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getLimit() {
            return limit;
        }

        public int getOffset() {
            return offset;
        }

        @Nonnull
        public Set<String> getScannableRecordTypeNames() {
            return recordLayerSchemaTemplate.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()); // refactor and cache.
        }

        @Nonnull
        public Set<String> getIndexNames() {
            return new HashSet<>(recordLayerSchemaTemplate.getTableIndexMapping().values()); // refactor and cache.
        }

        @Override
        protected void setExecuteProperties(@Nonnull final ExecuteProperties.Builder builder) {
            builder.setReturnedRowLimit(getLimit());
            builder.setSkip(getOffset());
        }
    }

    public static final class DDLContext extends AbstractContext {

        @Nonnull
        private final RecordLayerSchemaTemplate.Builder builder;
        private MetadataOperationsFactory metadataOperationsFactory;

        private DDLContext(@Nullable AbstractContext parent, @Nonnull final RecordLayerSchemaTemplate.Builder builder,
                           @Nonnull final MetadataOperationsFactory metadataOperationsFactory) {
            super(parent, TYPE.DDL);
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

        @Override
        void setExecuteProperties(@Nonnull ExecuteProperties.Builder builder) {
            // no-op
        }
    }

    public static final class DMLContext extends AbstractContext {

        @Nullable
        private Type targetType;

        @Nullable
        private StringTrieNode targetTypeReorderings;

        private DMLContext(@Nullable AbstractContext parent) {
            super(parent, TYPE.DML);
        }

        public void setTargetType(@Nonnull final Type targetType) {
            this.targetType = targetType;
        }

        @Nonnull
        public Type getTargetType() {
            Assert.thatUnchecked(hasTargetType(), "attempt to retrieve non-existing target type", ErrorCode.UNKNOWN_TYPE);
            return Verify.verifyNotNull(targetType);
        }

        public boolean hasTargetType() {
            return targetType != null;
        }

        public void setTargetTypeReorderings(@Nonnull final StringTrieNode targetTypeReorderings) {
            this.targetTypeReorderings = targetTypeReorderings;
        }

        @Nonnull
        public StringTrieNode getTargetTypeReorderings() {
            if (!hasTargetTypeReorderings()) {
                throw new RecordCoreException("attempt to retrieve non-existing target type reorderings");
            } else {
                return Verify.verifyNotNull(targetTypeReorderings);
            }
        }

        public boolean hasTargetTypeReorderings() {
            return targetTypeReorderings != null;
        }

        @Override
        void setExecuteProperties(@Nonnull ExecuteProperties.Builder builder) {
            // no-op
        }
    }
}
