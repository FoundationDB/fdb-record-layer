/*
 * PlanUtils.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class PlanUtils {
    public static final class KeyGenerator {

        @Nullable
        private String baseTable;

        private KeyGenerator() {
        }

        @Nonnull
        private KeyExpression visitLogicalSortExpression(@Nonnull LogicalSortExpression sortExpression) {
            Assert.thatUnchecked(sortExpression.getRelationalChildCount() == 1);
            Quantifier quantifier = sortExpression.getQuantifiers().get(0);
            Assert.thatUnchecked(quantifier.getRangesOver().get() instanceof SelectExpression);
            return visitSelectExpression(
                    (SelectExpression) quantifier.getRangesOver().get(),
                    sortExpression.getSortValues().stream().map(v -> (FieldValue) v).collect(Collectors.toList()));
        }

        @Nonnull
        private KeyExpression visitSelectExpression(@Nonnull SelectExpression selectExpression, List<FieldValue> orderBy) {
            Assert.thatUnchecked(selectExpression.getPredicates().isEmpty(), String.format("Unsupported index definition, found predicate in '%s'", selectExpression), ErrorCode.UNSUPPORTED_OPERATION);
            final var generator = getGenerator(selectExpression);

            Map<CorrelationIdentifier, Quantifier> projectedQuantifiers = selectExpression.getQuantifiers().stream().collect(Collectors.toMap(Quantifier::getAlias, Function.identity()));
            final List<FieldValue> projected = selectExpression.getResultValues().stream()
                    .filter(value -> !Collections.disjoint(value.getCorrelatedTo(), projectedQuantifiers.keySet()))
                    .map(value -> (FieldValue) value)
                    .collect(Collectors.toList());
            Assert.thatUnchecked(selectExpression.getResultValues().size() == projected.size(), String.format("Unsupported index definition, not all fields can be mapped to key expression in '%s'", selectExpression), ErrorCode.UNSUPPORTED_OPERATION);

            // Reorder columns based on orderBy clause
            List<FieldValue> orderedProjected = new ArrayList<>(orderBy);
            for (var field : projected) {
                if (!orderBy.contains(field)) {
                    orderedProjected.add(field);
                }
            }

            Assert.thatUnchecked(isProperlyClustered(orderedProjected), String.format("Unsupported index definition, improper column clustering in '%s'", selectExpression), ErrorCode.UNSUPPORTED_OPERATION);

            List<CorrelationIdentifier> orderedQuantifiers = orderedProjected.stream()
                    .map(f -> ((QuantifiedValue) f.getChild()).getAlias())
                    .distinct()
                    .collect(Collectors.toList());

            // Add fields for the quantifier or drill down to a nested expression
            final List<KeyExpression> parts = new ArrayList<>();
            int fieldIndex = 0;
            for (CorrelationIdentifier qunAlias : orderedQuantifiers) {
                Quantifier quantifier = projectedQuantifiers.remove(qunAlias);
                RelationalExpression child = quantifier.getRangesOver().get();
                if (child instanceof LogicalSortExpression) {
                    parts.add(visitLogicalSortExpression((LogicalSortExpression) child));
                    while (fieldIndex < orderedProjected.size() && ((QuantifiedValue) orderedProjected.get(fieldIndex).getChild()).getAlias().equals(qunAlias)) {
                        fieldIndex++;
                    }
                } else {
                    Assert.thatUnchecked(generator.isPresent() && generator.get().equals(quantifier),
                            String.format("Unsupported index definition, select statement can nest at most a single correlated join or table scan, found more than one in '%s'", selectExpression), ErrorCode.UNSUPPORTED_OPERATION);
                    for (; fieldIndex < orderedProjected.size() &&
                            ((QuantifiedValue) orderedProjected.get(fieldIndex).getChild()).getAlias().equals(qunAlias);
                            fieldIndex++) {
                        parts.add(Key.Expressions.field(orderedProjected.get(fieldIndex).getLastField().getFieldName()));
                    }
                    if (child instanceof LogicalTypeFilterExpression) {
                        visitLogicalTypeFilterExpression((LogicalTypeFilterExpression) child);
                    }
                }
            }

            // Combine KeyExpression parts
            KeyExpression result = parts.size() == 1 ? parts.get(0) : Key.Expressions.concat(parts);
            if (generator.isPresent()) {
                if (generator.get().getRangesOver().get() instanceof ExplodeExpression) {
                    final var explode = (ExplodeExpression) generator.get().getRangesOver().get();
                    final var collectionValueNested = explode.getCollectionValue();
                    Assert.thatUnchecked(collectionValueNested instanceof FieldValue, String.format("unexpected explode collection value of type '%s'", collectionValueNested.getClass().getSimpleName()));
                    final var fieldNested = (FieldValue) collectionValueNested;
                    return  Key.Expressions.field(fieldNested.getLastField().getFieldName(), KeyExpression.FanType.FanOut).nest(result);
                }
                Assert.thatUnchecked(generator.get().getRangesOver().get() instanceof LogicalTypeFilterExpression); // we should not nest, top-level index assumes definition starts here.
            }
            if (projected.size() != orderBy.size() && projected.size() > 1) {
                result = Key.Expressions.keyWithValue(result, orderBy.size());
            }
            return result;
        }

        private static boolean isProperlyClustered(List<FieldValue> fields) {
            Set<CorrelationIdentifier> seenQuantifiers = new HashSet<>();
            CorrelationIdentifier previous = ((QuantifiedValue) fields.get(0).getChild()).getAlias();
            seenQuantifiers.add(previous);
            for (FieldValue field : fields) {
                CorrelationIdentifier current = ((QuantifiedValue) field.getChild()).getAlias();
                if (!current.equals(previous)) {
                    if (seenQuantifiers.contains(current)) {
                        return false;
                    } else {
                        seenQuantifiers.add(current);
                        previous = current;
                    }
                }
            }
            return true;
        }

        @Nonnull
        public Optional<Quantifier> getGenerator(@Nonnull final SelectExpression selectExpression) {
            final var result = selectExpression.getQuantifiers().stream().filter(qun -> qun.getRangesOver().get() instanceof ExplodeExpression || qun.getRangesOver().get() instanceof LogicalTypeFilterExpression).collect(Collectors.toList());
            if (result.isEmpty()) {
                return Optional.empty();
            } else {
                Assert.thatUnchecked(result.size() == 1, String.format("Unsupported index definition, found more than iteration generator in '%s'", selectExpression), ErrorCode.UNSUPPORTED_OPERATION);
            }
            if (result.get(0).getRangesOver().get() instanceof LogicalTypeFilterExpression) {
                visitLogicalTypeFilterExpression((LogicalTypeFilterExpression) result.get(0).getRangesOver().get());
            }
            return Optional.of(result.get(0));
        }

        public void visitLogicalTypeFilterExpression(@Nonnull LogicalTypeFilterExpression element) {
            Assert.thatUnchecked(element.getRecordTypes().size() != 0, "type filter must scan at least one table", ErrorCode.INTERNAL_ERROR);
            Assert.thatUnchecked(element.getRecordTypes().size() == 1, String.format("Unsupported index definition, index can operate on a single table, however it is defined on more than one table '%s'", String.join(",", element.getRecordTypes())), ErrorCode.UNSUPPORTED_OPERATION);
            Assert.thatUnchecked(element.getRecordTypes().stream().findFirst().isPresent(), "type filter must scan at least one table", ErrorCode.INTERNAL_ERROR);
            final var filteredType = element.getRecordTypes().stream().findFirst().get();
            if (baseTable == null) {
                baseTable = element.getRecordTypes().stream().findFirst().get();
            } else {
                Assert.thatUnchecked(baseTable.equals(filteredType), String.format("Unsupported index definition, index can operate on a single table, however it is defined on more than one table '%s'", String.join(",", List.of(baseTable, filteredType))), ErrorCode.UNSUPPORTED_OPERATION);
            }
        }

        public static Pair<String, KeyExpression> evaluate(@Nonnull final RelationalExpression ref) {
            final var instance = new KeyGenerator();
            Assert.thatUnchecked(ref instanceof LogicalSortExpression, String.format("Unsupported index definition from query '%s'", ref));
            final var keyExpression = instance.visitLogicalSortExpression((LogicalSortExpression) ref);
            return Pair.of(instance.getBaseTable(), keyExpression);
        }

        @Nullable
        public String getBaseTable() {
            return baseTable;
        }
    }

    @Nonnull
    static Pair<String, KeyExpression> getMaterializedViewKeyDefinition(@Nonnull final RelationalExpression relationalExpression) {
        final String invalidIndex = "invalid index definition";
        Assert.thatUnchecked(relationalExpression instanceof LogicalSortExpression, invalidIndex);
        final var sortExpression = (LogicalSortExpression) relationalExpression;
        final var result = PlanUtils.KeyGenerator.evaluate(sortExpression);
        Assert.notNullUnchecked(result.getLeft(), String.format("could not generate a key expression from '%s'", relationalExpression));
        return Pair.of(result.getLeft(), result.getRight());
    }

    private PlanUtils() {
    }

}
