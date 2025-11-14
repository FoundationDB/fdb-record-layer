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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public OnSourceIndexGenerator(@Nonnull final Identifier indexName, @Nonnull final LogicalPlanFragment source,
                                  @Nonnull final List<IndexedColumn> keyColumns, @Nonnull final List<IndexedColumn> valueColumns,
                                  final boolean isUnique,
                                  final boolean useLegacyExtremum, final boolean useNullableArrays) {
        this.indexName = indexName;
        this.source = source;
        this.keyColumns = ImmutableList.copyOf(keyColumns);
        this.valueColumns = ImmutableList.copyOf(valueColumns);
        this.isUnique = isUnique;
        this.useLegacyExtremum = useLegacyExtremum;
        this.useNullableArrays = useNullableArrays;
    }

    @Nonnull
    public RecordLayerIndex generate(@Nonnull final RecordLayerSchemaTemplate catalog) {
        final var keyIdentifiers = keyColumns.stream().map(IndexedColumn::getIdentifier).collect(ImmutableList.toImmutableList());
        final var keyIdentifiersAsSet = ImmutableSet.copyOf(keyIdentifiers);
        final var valueIdentifiers = valueColumns.stream().map(IndexedColumn::getIdentifier)
                .filter(id -> !keyIdentifiersAsSet.contains(id)).collect(ImmutableList.toImmutableList());

        final var topLevelOperator = Iterables.getOnlyElement(source.getLogicalOperators());
        var topLevelSelect = topLevelOperator.getQuantifier().getRangesOver().get();
        Assert.thatUnchecked(topLevelSelect instanceof SelectExpression);

        final var resultValue = topLevelOperator.getQuantifier().getRangesOver().get().getResultValue();
        final Map<Identifier, Column<?>> originalOutputMap = topLevelOperator.getOutput().stream()
                .filter(e -> e.getName().isPresent())
                .collect(Collectors.toUnmodifiableMap(
                        expression -> expression.getName().get(),
                        expression -> {
                            final var name = expression.getName().map(Identifier::getName);
                            final var value = ImmutableList.of(expression.getUnderlying());
                            final var pushedDownValue = resultValue.pushDown(value, DefaultValueSimplificationRuleSet.instance(),
                                    EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(), topLevelOperator.getQuantifier().getAlias()).get(0);
                            return Column.of(name, pushedDownValue);
                        }));

        final List<Column<? extends Value>> projectionCols = ImmutableList.<Identifier>builder().addAll(keyIdentifiers).addAll(valueIdentifiers)
                .build().stream().map(identifier -> {
                    final var column = originalOutputMap.get(identifier);
                    Assert.notNullUnchecked(column, ErrorCode.UNDEFINED_COLUMN, () -> "could not find " + identifier);
                    return column;
                }).collect(ImmutableList.toImmutableList());

        final var orderByExpressions = keyColumns.stream().map(keyColumn -> {
            final var column = originalOutputMap.get(keyColumn.identifier);
            Assert.notNullUnchecked(column, ErrorCode.UNDEFINED_COLUMN, () -> "could not find " + keyColumn.identifier);
            return OrderByExpression.of(Expression.fromColumn(column), keyColumn.isDescending, keyColumn.isNullsLast);
        }).collect(ImmutableList.toImmutableList());


        final var originalSelectExpression = (SelectExpression)topLevelSelect;
        final var newSelectExpression = GraphExpansion.builder().addAllQuantifiers(originalSelectExpression.getQuantifiers())
                .addAllPredicates(originalSelectExpression.getPredicates())
                .addAllResultColumns(projectionCols)
                .build().buildSelect();

        final var projectionExpressions = Expressions.of(projectionCols.stream().map(Expression::fromColumn).collect(ImmutableList.toImmutableList()));

        final var resultingOperator = LogicalOperator.newUnnamedOperator(projectionExpressions, Quantifier.forEach(Reference.initialOf(newSelectExpression)));
        final var indexPlan = LogicalOperator.generateSort(resultingOperator, orderByExpressions, ImmutableSet.of(), Optional.empty());
        final var indexGenerator = MaterializedViewIndexGenerator.from(indexPlan.getQuantifier().getRangesOver().get(), useLegacyExtremum);
        final var tableType = Assert.castUnchecked(catalog.findTableByName(indexGenerator.getRecordTypeName()).get(), RecordLayerTable.class);
        return indexGenerator.generate(indexName.toString(), isUnique, tableType.getType(), useNullableArrays);
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

        private boolean isUnique;

        private boolean useLegacyExtremum;

        private boolean useNullableArrays;

        private Builder() {
            this.keyColumns = new ArrayList<>();
            this.valueColumns = new ArrayList<>();
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
        public Builder setIndexType(@Nonnull final String indexType) {
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
        public OnSourceIndexGenerator build() {
            Assert.notNullUnchecked(indexName);
            Assert.notNullUnchecked(indexSource);
            Assert.notNullUnchecked(semanticAnalyzer);
            Assert.thatUnchecked(!keyColumns.isEmpty());
            return new OnSourceIndexGenerator(indexName, indexSource, keyColumns, valueColumns,
                    isUnique, useLegacyExtremum, useNullableArrays);
        }
    }
}
