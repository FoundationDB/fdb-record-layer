/*
 * IndexBuilder.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.visitors.IdentifierVisitor;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generator for creating indexes using the {@code INDEX ON} syntax.
 * <p>
 * This class is responsible for processing and generating {@link RecordLayerIndex} instances from the
 * {@code INDEX ON} DDL syntax, which allows creating indexes directly on table sources with explicit
 * column specifications. This is in contrast to the {@code INDEX AS} syntax which defines indexes through
 * arbitrary SELECT queries.
 * <p>
 * The {@code INDEX ON} syntax follows this form:
 * <pre>
 * [UNIQUE] INDEX index_name ON table_name (
 *     column1 [ASC|DESC] [NULLS FIRST|NULLS LAST],
 *     column2 [ASC|DESC] [NULLS FIRST|NULLS LAST],
 *     ...
 * )
 * [INCLUDE (value_column1, value_column2, ...)]
 * [OPTIONS (option_name, ...)]
 * </pre>
 * <p>
 * The index definition consists of:
 * <ul>
 *     <li><b>Key columns:</b> The ordered list of columns in the index key, specified in the main column list.
 *         Each column can have optional ordering modifiers (ASC/DESC, NULLS FIRST/LAST).</li>
 *     <li><b>Value columns:</b> Additional columns to store in the index but not part of the key, specified
 *         in the optional INCLUDE clause. These columns can be used to satisfy queries without accessing
 *         the base table (covering index pattern).</li>
 *     <li><b>Uniqueness constraint:</b> Optional UNIQUE modifier to enforce that key column combinations
 *         are unique across the indexed table.</li>
 *     <li><b>Index options:</b> Optional configuration flags such as LEGACY_EXTREMUM_EVER or USE_NULLABLE_ARRAYS
 *         that affect index behavior and storage.</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * CREATE UNIQUE INDEX idx_user_email ON users (email ASC)
 *     INCLUDE (first_name, last_name)
 *     OPTIONS (LEGACY_EXTREMUM_EVER);
 * </pre>
 * <p>
 * The generator transforms the source table query into a properly ordered and projected logical plan,
 * then delegates to {@link MaterializedViewIndexGenerator} to produce the final index structure. The
 * generated index maintains the specified column ordering and can leverage the query planner for
 * efficient index-based query execution.
 * <p>
 * This class is typically constructed via its {@link Builder} pattern, which validates that all required
 * components (index name, source query, and at least one key column) are provided before generation.
 *
 * @see MaterializedViewIndexGenerator
 * @see RecordLayerIndex
 */
public final class OnSourceIndexGenerator {

    @Nonnull
    private final Identifier indexName;

    @Nonnull
    private final List<IndexedColumn> keyColumns;

    @Nonnull
    private final List<IndexedColumn> valueColumns;

    @Nonnull
    private final LogicalPlanFragment source;

    private final boolean isUnique;

    private final boolean useLegacyExtremum;

    private final boolean useNullableArrays;

    private final boolean generateKeyValueExpressionWithEmptyKey;

    @Nonnull
    private final Map<String, String> indexOptions;

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder metadataBuilder;

    public OnSourceIndexGenerator(@Nonnull final Identifier indexName, @Nonnull final LogicalPlanFragment source,
                                  @Nonnull final List<IndexedColumn> keyColumns, @Nonnull final List<IndexedColumn> valueColumns,
                                  final boolean isUnique, final boolean useLegacyExtremum, final boolean useNullableArrays,
                                  final boolean generateKeyValueExpressionWithEmptyKey, @Nonnull final Map<String, String> indexOptions,
                                  @Nonnull final RecordLayerSchemaTemplate.Builder metadataBuilder) {
        this.indexName = indexName;
        this.source = source;
        this.keyColumns = ImmutableList.copyOf(keyColumns);
        this.valueColumns = ImmutableList.copyOf(valueColumns);
        this.isUnique = isUnique;
        this.useLegacyExtremum = useLegacyExtremum;
        this.useNullableArrays = useNullableArrays;
        this.generateKeyValueExpressionWithEmptyKey = generateKeyValueExpressionWithEmptyKey;
        this.indexOptions = ImmutableMap.copyOf(indexOptions);
        this.metadataBuilder = metadataBuilder;
    }

    /**
     * Generates a {@link RecordLayerIndex} based on the configured source query and index columns.
     * <p>
     * This method first extracts key and value column identifiers from the configured columns, ensuring value
     * columns don't duplicate key columns. It then retrieves the top-level logical operator from the source query
     * plan and builds a mapping of column identifiers to their underlying {@link Column} representations, pushing
     * down values through the query plan. A projection is created containing all key and value columns in order,
     * and ORDER BY expressions are constructed from the key columns, respecting their ordering specifications
     * (ASC/DESC, NULLS FIRST/LAST). The SELECT expression is then reconstructed with the projected columns while
     * preserving the original quantifiers and predicates. Finally, a sorted logical plan is generated based on
     * the ORDER BY expressions, and the method delegates to {@link MaterializedViewIndexGenerator} to produce
     * the final index structure.
     * <p>
     * The generated index will be ordered according to the key columns and can optionally enforce uniqueness
     * if configured via {@link Builder#setUnique(boolean)}.
     *
     * @return a fully configured {@link RecordLayerIndex} ready to be added to the schema
     */
    @Nonnull
    public RecordLayerIndex.Builder generate() {
        final var keyIdentifiers = keyColumns.stream().map(IndexedColumn::getIdentifier).collect(ImmutableList.toImmutableList());
        final var keyIdentifiersAsSet = ImmutableSet.copyOf(keyIdentifiers);
        final var valueIdentifiers = valueColumns.stream().map(IndexedColumn::getIdentifier)
                .filter(id -> !keyIdentifiersAsSet.contains(id)).collect(ImmutableList.toImmutableList());

        final var topLevelOperator = Iterables.getOnlyElement(source.getLogicalOperators());
        final var topLevelSelect = topLevelOperator.getQuantifier().getRangesOver().get();
        Assert.thatUnchecked(topLevelSelect instanceof SelectExpression);

        final var selectResultValue = topLevelOperator.getQuantifier().getRangesOver().get().getResultValue();
        final Map<Identifier, Column<?>> originalOutputMap = topLevelOperator.getOutput().stream()
                .filter(e -> e.getName().isPresent())
                .collect(Collectors.toUnmodifiableMap(
                        expression -> expression.getName().get(),
                        expression -> {
                            final var value = ImmutableList.of(expression.getUnderlying());
                            final var pushedDownValue = selectResultValue.pushDown(value, DefaultValueSimplificationRuleSet.instance(),
                                    EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(),
                                    topLevelOperator.getQuantifier().getAlias()).get(0);
                            final var name = expression.getName().map(Identifier::getName);
                            return Column.of(name, pushedDownValue);
                        }));

        final List<Column<? extends Value>> projectionCols = ImmutableList.<Identifier>builder()
                .addAll(keyIdentifiers)
                .addAll(valueIdentifiers)
                .build().stream().map(identifier -> {
                    final var column = originalOutputMap.get(identifier);
                    Assert.notNullUnchecked(column, ErrorCode.UNDEFINED_COLUMN, () -> "could not find " + identifier);
                    return column;
                }).collect(ImmutableList.toImmutableList());

        final List<OrderByExpression> orderByExpressions = keyColumns.stream().map(keyColumn -> {
            final var column = originalOutputMap.get(keyColumn.getIdentifier());
            Assert.notNullUnchecked(column, ErrorCode.UNDEFINED_COLUMN, () -> "could not find " + keyColumn.getIdentifier());
            return OrderByExpression.of(Expression.fromColumn(column), keyColumn.isDescending(), keyColumn.isNullsLast());
        }).collect(ImmutableList.toImmutableList());


        final var originalSelectExpression = (SelectExpression)topLevelSelect;
        final var newSelectExpression = GraphExpansion.builder()
                .addAllQuantifiers(originalSelectExpression.getQuantifiers())
                .addAllPredicates(originalSelectExpression.getPredicates())
                .addAllResultColumns(projectionCols)
                .build().buildSelect();

        final var projectionExpressions = Expressions.of(projectionCols.stream()
                .map(Expression::fromColumn)
                .collect(ImmutableList.toImmutableList()));

        final var resultingOperator = LogicalOperator.newUnnamedOperator(projectionExpressions,
                Quantifier.forEach(Reference.initialOf(newSelectExpression)));
        final var indexPlan = LogicalOperator.generateSort(resultingOperator, orderByExpressions, ImmutableSet.of(), Optional.empty());
        final var indexGenerator = MaterializedViewIndexGenerator.from(indexPlan.getQuantifier().getRangesOver().get(), useLegacyExtremum);
        final var indexBuilder = indexGenerator.generate(metadataBuilder, indexName.toString(), isUnique, useNullableArrays, generateKeyValueExpressionWithEmptyKey);
        indexBuilder.addAllOptions(indexOptions);
        return indexBuilder;
    }

    public static final class IndexedColumn {

        @Nonnull
        private final Identifier identifier;

        private final boolean isDescending;

        private final boolean isNullsLast;

        private IndexedColumn(@Nonnull final Identifier identifier, boolean isDescending, boolean isNullsLast) {
            this.identifier = identifier;
            this.isDescending = isDescending;
            this.isNullsLast = isNullsLast;
        }

        @Nonnull
        public Identifier getIdentifier() {
            return identifier;
        }

        public boolean isDescending() {
            return isDescending;
        }

        public boolean isNullsLast() {
            return isNullsLast;
        }

        @Nonnull
        public static IndexedColumn of(@Nonnull final Identifier identifier, boolean isDescending, boolean isNullsLast) {
            return new IndexedColumn(identifier, isDescending, isNullsLast);
        }

        /**
         * Parses an index column specification from a parser context into an {@link IndexedColumn}.
         * <p>
         * This method extracts the column identifier and optional ordering information (ASC/DESC and NULLS FIRST/LAST)
         * from the parser context. If no order clause is specified, defaults to ascending order with nulls first.
         * When an order clause is present but nulls ordering is not explicitly specified, the default behavior is:
         * <ul>
         *     <li>For DESC columns: NULLS LAST</li>
         *     <li>For ASC columns: NULLS FIRST</li>
         * </ul>
         *
         * @param columnSpec the parser context containing the column specification
         * @param identifierVisitor the visitor used to extract the column identifier
         * @return an {@link IndexedColumn} representing the parsed column specification
         */
        @Nonnull
        public static OnSourceIndexGenerator.IndexedColumn parseColSpec(@Nonnull final RelationalParser.IndexColumnSpecContext columnSpec,
                                                                        @Nonnull final IdentifierVisitor identifierVisitor) {
            final var columnId = identifierVisitor.visitUid(columnSpec.columnName);
            final var orderContext = columnSpec.orderClause();

            boolean isDesc = false;
            boolean nullsLast = false;

            if (orderContext == null) {
                return OnSourceIndexGenerator.IndexedColumn.of(columnId, isDesc, nullsLast);
            }

            isDesc = orderContext.DESC() != null;
            if (orderContext.nulls == null) {
                nullsLast = isDesc;
            } else {
                nullsLast = orderContext.LAST() != null;
            }
            return OnSourceIndexGenerator.IndexedColumn.of(columnId, isDesc, nullsLast);
        }

        @Nonnull
        public static OnSourceIndexGenerator.IndexedColumn parseUid(@Nonnull final RelationalParser.UidContext uid,
                                                                    @Nonnull final IdentifierVisitor identifierVisitor) {
            final var columnId = identifierVisitor.visitUid(uid);
            return OnSourceIndexGenerator.IndexedColumn.of(columnId, false, false);
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Identifier indexName;

        private LogicalPlanFragment indexSource;

        private SemanticAnalyzer semanticAnalyzer;

        @Nonnull
        private final List<IndexedColumn> keyColumns;

        @Nonnull
        private final List<IndexedColumn> valueColumns;

        @Nonnull
        private final Map<String, String> indexOptions;

        private boolean isUnique;

        private boolean useLegacyExtremum;

        private boolean useNullableArrays;

        private boolean generateKeyValueExpressionWithEmptyKey;

        private RecordLayerSchemaTemplate.Builder metadataBuilder;

        private Builder() {
            this.keyColumns = new ArrayList<>();
            this.valueColumns = new ArrayList<>();
            this.indexOptions = new HashMap<>();
        }

        @Nonnull
        public Builder setIndexName(@Nonnull final Identifier indexName) {
            this.indexName = indexName;
            return this;
        }

        @Nonnull
        public Builder setIndexSource(@Nonnull final LogicalPlanFragment indexSource) {
            this.indexSource = indexSource;
            return this;
        }

        @Nonnull
        public Builder setSemanticAnalyzer(@Nonnull final SemanticAnalyzer semanticAnalyzer) {
            this.semanticAnalyzer = semanticAnalyzer;
            return this;
        }

        @Nonnull
        public Builder addKeyColumn(@Nonnull final IndexedColumn keyColumn) {
            keyColumns.add(keyColumn);
            return this;
        }

        @Nonnull
        public Builder addValueColumn(@Nonnull final IndexedColumn keyColumn) {
            valueColumns.add(keyColumn);
            return this;
        }

        @Nonnull
        public List<IndexedColumn> getValueColumns() {
            return valueColumns;
        }

        @Nonnull
        public Builder addIndexOption(@Nonnull final String key, @Nonnull final String value) {
            indexOptions.put(key, value);
            return this;
        }

        @Nonnull
        public Builder addAllIndexOptions(@Nonnull final Map<String, String> indexOptions) {
            this.indexOptions.putAll(indexOptions);
            return this;
        }

        @Nonnull
        public Builder setUnique(boolean isUnique) {
            this.isUnique = isUnique;
            return this;
        }

        @Nonnull
        public Builder setUseLegacyExtremum(boolean useLegacyExtremum) {
            this.useLegacyExtremum = useLegacyExtremum;
            return this;
        }

        @Nonnull
        public Builder setUseNullableArrays(boolean useNullableArrays) {
            this.useNullableArrays = useNullableArrays;
            return this;
        }

        @Nonnull
        public Builder setGenerateKeyValueExpressionWithEmptyKey(boolean generateKeyValueExpressionWithEmptyKey) {
            this.generateKeyValueExpressionWithEmptyKey = generateKeyValueExpressionWithEmptyKey;
            return this;
        }


        @Nonnull
        public Builder setMetadataBuilder(final RecordLayerSchemaTemplate.Builder metadataBuilder) {
            this.metadataBuilder = metadataBuilder;
            return this;
        }

        @Nonnull
        public OnSourceIndexGenerator build() {
            Assert.notNullUnchecked(indexName);
            Assert.notNullUnchecked(indexSource);
            Assert.notNullUnchecked(semanticAnalyzer);
            Assert.notNullUnchecked(metadataBuilder);
            return new OnSourceIndexGenerator(indexName, indexSource, keyColumns, valueColumns,
                    isUnique, useLegacyExtremum, useNullableArrays, generateKeyValueExpressionWithEmptyKey,
                    indexOptions, metadataBuilder);
        }
    }
}
