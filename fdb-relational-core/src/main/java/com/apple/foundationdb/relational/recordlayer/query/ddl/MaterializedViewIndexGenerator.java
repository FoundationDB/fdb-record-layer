/*
 * MaterializedViewIndexGenerator.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;

/**
 * Builds a {@link RecordLayerIndex} from the plan of an index-defining query, out of what three passes produce:
 * {@link QuantifierValues} maps the plan's quantifiers, {@link IndexSpec} collects and validates what the index is made
 * of, and {@link ValueToKeyExpressionVisitor} turns the projection into the key.
 */
@API(API.Status.EXPERIMENTAL)
public final class MaterializedViewIndexGenerator implements IndexGenerator {

    @Nonnull
    private final RelationalExpression relationalExpression;

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder;

    @Nonnull
    private final String indexName;

    @Nonnull
    private final IndexGenerationOptions options;

    private MaterializedViewIndexGenerator(@Nonnull RelationalExpression relationalExpression,
                                           @Nonnull RecordLayerSchemaTemplate.Builder schemaTemplateBuilder,
                                           @Nonnull String indexName,
                                           @Nonnull IndexGenerationOptions options) {
        this.relationalExpression = relationalExpression;
        this.schemaTemplateBuilder = schemaTemplateBuilder;
        this.indexName = indexName;
        this.options = options;
    }

    /**
     * A generator for one index definition.
     *
     * @param relationalExpression the plan of the index-defining query
     * @param schemaTemplateBuilder the metadata the index is added to
     * @param indexName the name the definition gives the index
     * @param options what the definition asks of the index beyond its key
     *
     * @return a generator for that definition
     */
    @Nonnull
    public static MaterializedViewIndexGenerator newInstance(@Nonnull RelationalExpression relationalExpression,
                                                            @Nonnull RecordLayerSchemaTemplate.Builder schemaTemplateBuilder,
                                                            @Nonnull String indexName,
                                                            @Nonnull IndexGenerationOptions options) {
        return new MaterializedViewIndexGenerator(relationalExpression, schemaTemplateBuilder, indexName, options);
    }

    @Nonnull
    @Override
    public RecordLayerIndex.Builder generate() {
        final var spec = IndexSpec.collect(relationalExpression,
                QuantifierValues.collect(relationalExpression));
        spec.checkValidity();

        final var translation = translateToKeyExpression(spec);
        final var indexType = translation.indexType();
        // the record layer indexes by storage name
        final var tableType = schemaTemplateBuilder.findTableByStorageName(spec.recordTypeName()).getType();

        final var indexBuilder = RecordLayerIndex.newBuilder()
                .setName(indexName)
                .setTableType(tableType)
                .setUnique(options.unique())
                .setIndexType(indexType);
        final var predicate = spec.predicate();
        if (predicate != null) {
            indexBuilder.setPredicate(IndexPredicate.fromQueryPredicate(predicate).toProto());
        }

        var keyExpression = translation.keyExpression();
        if (spec.projection().aggregate() == null) {
            keyExpression = splitKeyFromValue(spec, keyExpression, options.emptyKeyAllowed());
        } else {
            addAggregatePermutationOptions(indexBuilder, spec, indexType);
        }
        indexBuilder.setKeyExpression(KeyExpression.fromProto(
                NullableArrayUtils.wrapArray(keyExpression.toKeyExpression(), tableType, options.containsNullableArray())));
        return indexBuilder;
    }

    /**
     * Translates the projection into the index key, columns in key order: the order-by columns lead a value index, while
     * an aggregate index keeps the projection's order.
     */
    @Nonnull
    private ValueToKeyExpressionVisitor.Result translateToKeyExpression(@Nonnull final IndexSpec spec) {
        final var projection = spec.projection();
        final var isAggregate = projection.aggregate() != null;
        final var reorderedValues = isAggregate ? projection.values()
                                          : reorderValues(projection.fieldValues(), spec.getOrderByValues());
        return ValueToKeyExpressionVisitor.translate(RecordConstructorValue.ofUnnamed(reorderedValues),
                isAggregate ? Map.of() : spec.getOrderingFunctions(), options.extremumEverStorage());
    }

    @Nonnull
    private static List<Value> reorderValues(@Nonnull final List<Value> allValues, @Nonnull final List<Value> keyValues) {
        Assert.thatUnchecked(allValues.size() >= keyValues.size());
        if (keyValues.isEmpty()) {
            return allValues;
        }
        final var valueValues = allValues.stream()
                .filter(value -> !keyValues.contains(value))
                .collect(ImmutableList.toImmutableList());
        return ImmutableList.<Value>builder().addAll(keyValues).addAll(valueValues).build();
    }

    /**
     * Stores the columns beyond the ordered ones as the index's value rather than as part of its key. With no ordering
     * clause the caller says whether the key is empty or there is no split at all.
     */
    @Nonnull
    private static KeyExpression splitKeyFromValue(@Nonnull final IndexSpec spec, @Nonnull final KeyExpression keyExpression,
                                                   final boolean emptyKeyAllowed) {
        final var splitPoint = spec.getOrderByValues().size();
        if (splitPoint == 0 && !emptyKeyAllowed) {
            return keyExpression;
        }
        return splitPoint < spec.projection().fieldValues().size()
               ? keyWithValue(keyExpression, splitPoint) : keyExpression;
    }

    /**
     * Tells a permuted index how many columns follow the aggregate in its key. Every other aggregate index type keeps the
     * aggregate last and cannot be ordered by it.
     */
    private static void addAggregatePermutationOptions(@Nonnull final RecordLayerIndex.Builder indexBuilder,
                                                       @Nonnull final IndexSpec spec, @Nonnull final String indexType) {
        final var aggregateOrderIndex = spec.aggregateOrderIndex();
        if (IndexTypes.PERMUTED_MIN.equals(indexType) || IndexTypes.PERMUTED_MAX.equals(indexType)) {
            indexBuilder.setOption(IndexOptions.PERMUTED_SIZE_OPTION,
                    aggregateOrderIndex < 0 ? 0 : spec.projection().fieldValues().size() - aggregateOrderIndex);
        } else {
            Assert.thatUnchecked(aggregateOrderIndex <= 0, ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition. Cannot order " + indexType + " index by aggregate value");
        }
    }
}
