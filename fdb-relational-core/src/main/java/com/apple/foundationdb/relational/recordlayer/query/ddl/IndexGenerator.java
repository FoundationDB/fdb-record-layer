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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;
import static java.util.stream.Collectors.toList;

public final class IndexGenerator {

    @Nonnull
    private final Identifier indexName;

    @Nonnull
    private final List<IndexedColumn> keyColumns;

    @Nonnull
    private final List<IndexedColumn> valueColumns;

    @Nonnull
    private final String indexType;

    @Nonnull
    private final Map<String, String> indexOptions;

    @Nonnull
    private final RelationalExpression source;

    @Nonnull
    private final SemanticAnalyzer semanticAnalyzer;

    private final boolean isUnique;

    private final boolean useLegacyExtremum;

    public IndexGenerator(@Nonnull final Identifier indexName, @Nonnull final RelationalExpression source, @Nonnull final SemanticAnalyzer semanticAnalyzer,
                          @Nonnull final List<IndexedColumn> keyColumns, @Nonnull final List<IndexedColumn> valueColumns,
                          @Nonnull final String indexType, @Nonnull final Map<String, String> indexOptions, final boolean isUnique,
                          final boolean useLegacyExtremum) {
        this.indexName = indexName;
        this.source = source;
        this.semanticAnalyzer = semanticAnalyzer;
        this.keyColumns = ImmutableList.copyOf(keyColumns);
        this.valueColumns = ImmutableList.copyOf(valueColumns);
        this.indexType = indexType;
        this.indexOptions = ImmutableMap.copyOf(indexOptions);
        this.isUnique = isUnique;
        this.useLegacyExtremum = useLegacyExtremum;
    }

    @Nonnull
    public RecordLayerIndex generate() {
        final var recordLayerIndexBuilder = buildKeyExpression();
        return recordLayerIndexBuilder.setName(indexName.getName())
                .setUnique(isUnique)
                .setIndexType(indexType)
                .addAllOptions(indexOptions)
                .build();
    }

    @Nonnull
    private RecordLayerIndex.Builder buildKeyExpression() {
        final var derivedSourceValue = new ValueLineageVisitor().visit(source).simplify(EvaluationContext.empty(), AliasMap.emptyMap(), Set.of());
        final var derivedSourceExpression = Expressions.of(Assert.castUnchecked(derivedSourceValue, RecordConstructorValue.class)
                .getColumns().stream().map(Expression::fromColumn).collect(ImmutableList.toImmutableList()));

        final var keyExpressionDecoratorBuilder = KeyExpressionBuilder.KeyExpressionDecorator.newBuilder()
                .setUseLegacyExtremum(useLegacyExtremum);
        final var indexedExpressions = Streams.concat(
                        keyColumns.stream().map(keyColumn -> {
                            final var value = Assert.optionalUnchecked(semanticAnalyzer.lookupAlias(keyColumn.identifier, derivedSourceExpression),
                                    ErrorCode.UNDEFINED_COLUMN, () -> "Attempting to index non existing column '" + keyColumn.identifier + "'");
                            if (keyColumn.columnSort != ColumnSort.Undefined) {
                                keyExpressionDecoratorBuilder.addOrderKeyExpression(value.getUnderlying(), keyColumn.columnSort);
                            }
                            return value;
                        }),
                        valueColumns.stream().map(includeUid ->
                            Assert.optionalUnchecked(semanticAnalyzer.lookupAlias(includeUid.identifier, derivedSourceExpression),
                                    ErrorCode.UNDEFINED_COLUMN, () -> "Attempting to index non existing column '" + includeUid.identifier + "'")))
                .collect(ImmutableList.toImmutableList());
        final var indexExpr = Expressions.of(indexedExpressions);
        final var rcv = RecordConstructorValue.ofColumns(indexExpr.underlyingAsColumns());
        if (!valueColumns.isEmpty()) {
            keyExpressionDecoratorBuilder.addKeyValueExpression(rcv, keyColumns.size());
        }

        final var keyExpressionBuilder = KeyExpressionBuilder.buildKeyExpression(rcv, keyExpressionDecoratorBuilder.build());
        final var indexBuilder = RecordLayerIndex.newBuilder();

        indexBuilder.setKeyExpression(keyExpressionBuilder.getKeyExpression())
                .setIndexType(keyExpressionBuilder.getIndexType())
                .setTableName(keyExpressionBuilder.getBaseTypeName());

        @Nullable var indexPredicate = getIndexPredicate();
        if (indexPredicate != null) {
            indexBuilder.setPredicate(indexPredicate);
        }

        return indexBuilder;
    }

    @Nullable
    private RecordMetaDataProto.Predicate getIndexPredicate() {
        final var partialOrder = referencesAndDependencies().evaluate(Reference.initialOf(source));
        final var expressionRefs =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles")).stream().map(Reference::get).collect(toList());
        final var predicate = LegacyIndexGenerator.getTopLevelPredicate(Lists.reverse(expressionRefs));
        if (predicate == null) {
            return null;
        }
        return IndexPredicate.fromQueryPredicate(predicate).toProto();
    }

    public static final class IndexedColumn {

        @Nonnull
        private final Identifier identifier;

        @Nonnull
        private final ColumnSort columnSort;

        private IndexedColumn(@Nonnull final Identifier identifier,
                      @Nonnull final ColumnSort sortCriteria) {
            this.identifier = identifier;
            this.columnSort = sortCriteria;
        }

        @Nonnull
        public static IndexedColumn of(@Nonnull final Identifier identifier) {
            return new IndexedColumn(identifier, ColumnSort.Undefined);
        }

        @Nonnull
        public static IndexedColumn of(@Nonnull final Identifier identifier, ColumnSort sort) {
            return new IndexedColumn(identifier, sort);
        }

        @Nonnull
        public static IndexedColumn of(@Nonnull final Identifier identifier, boolean isDescending, boolean isNullsLast) {
            return new IndexedColumn(identifier, ColumnSort.of(isDescending, isNullsLast));
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Identifier indexName;

        private RelationalExpression indexSource;

        private SemanticAnalyzer semanticAnalyzer;

        @Nonnull
        private final List<IndexedColumn> keyColumns;

        @Nonnull
        private final List<IndexedColumn> valueColumns;

        @Nonnull
        private final Map<String, String> indexOptions;

        private String indexType;

        private boolean isUnique;

        private boolean useLegacyExtremum;

        private Builder() {
            this.keyColumns = new ArrayList<>();
            this.valueColumns = new ArrayList<>();
            this.indexOptions = new LinkedHashMap<>();
        }

        @Nonnull
        public Builder setIndexName(@Nonnull final Identifier indexName) {
            this.indexName = indexName;
            return this;
        }

        @Nonnull
        public Builder setIndexSource(@Nonnull final RelationalExpression relationalExpression) {
            indexSource = relationalExpression;
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
        public Builder addIndexOption(@Nonnull final String key, @Nonnull final String value) {
            indexOptions.put(key, value);
            return this;
        }

        @Nonnull
        public Builder setIndexType(@Nonnull final String indexType) {
            this.indexType = indexType;
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
        public IndexGenerator build() {
            Assert.notNullUnchecked(indexName);
            Assert.notNullUnchecked(indexSource);
            Assert.notNullUnchecked(semanticAnalyzer);
            Assert.thatUnchecked(!keyColumns.isEmpty());
            if (indexType == null) {
                indexType = IndexTypes.VALUE;
            }
            return new IndexGenerator(indexName, indexSource, semanticAnalyzer, keyColumns, valueColumns, indexType,
                    indexOptions, isUnique, useLegacyExtremum);
        }
    }
}
