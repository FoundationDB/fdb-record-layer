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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class PlanUtils {
    public static final class KeyGenerator {

        @Nullable
        private String baseTable;

        private KeyGenerator() {
        }

        @Nonnull
        public KeyExpression visitSelectExpression(@Nonnull SelectExpression element) {
            Assert.thatUnchecked(element.getPredicates().isEmpty(), String.format("Unsupported index definition, found predicate in '%s'", element), ErrorCode.UNSUPPORTED_OPERATION);
            final var generator = getGenerator(element);
            final List<KeyExpression> parts = new ArrayList<>();
            // we should iterate based on the order of columns in the select, we could change that later to use an explicit ORDER BY for example.
            final var sorted = element.getQuantifiers().stream().map(q -> Pair.of(projectedFieldsOf(element, q), q)).filter(p -> !p.getLeft().isEmpty()).sorted(
                    Comparator.comparingInt(p -> (p.getLeft().stream().min(Comparator.comparingInt(Pair::getLeft))).get().getLeft())).collect(Collectors.toList());
            // number of fields must match what the select contains.
            Assert.thatUnchecked(element.getResultValues().size() == sorted.stream().mapToInt(p -> p.getLeft().size()).sum(), String.format("Unsupported index definition, not all fields can be mapped to key expression in '%s'", element), ErrorCode.UNSUPPORTED_OPERATION);
            sorted.forEach(pair -> {
                final var qun = pair.getRight();
                final var qunFields = pair.getLeft();
                final var child = qun.getRangesOver().get();
                if (child instanceof SelectExpression) {
                    final var part = visitSelectExpression((SelectExpression) qun.getRangesOver().get());
                    parts.add(part);
                } else {
                    Assert.thatUnchecked(generator.isPresent() && generator.get().equals(qun),
                            String.format("Unsupported index definition, select statement can nest at most a single correlated join or table scan, found more than one in '%s'", element), ErrorCode.UNSUPPORTED_OPERATION);
                    parts.addAll(qunFields.stream().map(Pair::getRight).map(Key.Expressions::field).collect(Collectors.toList()));
                    if (child instanceof LogicalTypeFilterExpression) {
                        visitLogicalTypeFilterExpression((LogicalTypeFilterExpression) child);
                    }
                }
            });
            final var result = parts.size() == 1 ? parts.get(0) : Key.Expressions.concat(parts);
            if (generator.isPresent()) {
                if (generator.get().getRangesOver().get() instanceof ExplodeExpression) {
                    final var explode = (ExplodeExpression) generator.get().getRangesOver().get();
                    final var collectionValueNested = explode.getCollectionValue();
                    Assert.thatUnchecked(collectionValueNested instanceof FieldValue, String.format("unexpected explode collection value of type '%s'", collectionValueNested.getClass().getSimpleName()));
                    final var fieldNested = (FieldValue) collectionValueNested;
                    return  Key.Expressions.field(fieldNested.getFieldName(), KeyExpression.FanType.FanOut).nest(result);
                }
                Assert.thatUnchecked(generator.get().getRangesOver().get() instanceof LogicalTypeFilterExpression); // we should not nest, top-level index assumes definition starts here.
            }
            return result;
        }

        private List<Pair<Integer, String>> projectedFieldsOf(SelectExpression element, Quantifier qun) {
            final var fields = IntStream.range(0, element.getResultValues().size()).filter(i -> element.getResultValues().get(i).getCorrelatedTo().contains(qun.getAlias())).mapToObj(i -> Pair.of(i, ((FieldValue) element.getResultValues().get(i)).getFieldName())).collect(Collectors.toList());
            if (fields.isEmpty()) {
                return List.of();
            }

            // make sure columns are adjacent to one another.
            {
                final var min = fields.stream().map(Pair::getLeft).min(Integer::compare).get();
                Assert.thatUnchecked(min.equals(fields.get(0).getLeft()) || min.equals(fields.get(fields.size() - 1).getLeft()),
                        String.format("Unsupported index definition, improper column clustering in '%s'", element), ErrorCode.UNSUPPORTED_OPERATION);
                final int step = min.equals(fields.get(fields.size() - 1).getLeft()) ? 1 : -1;
                final var isConsecutive = Streams.zip(fields.stream().map(Pair::getLeft), fields.stream().map(Pair::getLeft).skip(1), (a, b) -> a - b).allMatch(i -> i.equals(step));
                Assert.thatUnchecked(isConsecutive, String.format("Unsupported index definition, improper column clustering in '%s'", element), ErrorCode.UNSUPPORTED_OPERATION);
            }

            // make sure columns have the same order as their underlying.
            if (qun.getRangesOver().get() instanceof SelectExpression) {
                final var underlyingColNames = qun.getFlowedColumns().stream().map(c -> c.getField().getFieldName()).collect(Collectors.toList());
                final var colNames = fields.stream().sorted(Comparator.comparing(Pair::getLeft)).map(Pair::getRight).collect(Collectors.toList());
                Assert.thatUnchecked(colNames.equals(underlyingColNames), String.format("Unsupported index definition, order of underlying projected columns changed '%s'", element));
            }
            return fields;
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
            Assert.thatUnchecked(element.getRecordTypes().size() == 1, String.format("Unsupported index definition, materialized view can operate on a single table, however it is defined on more than one table '%s'", String.join(",", element.getRecordTypes())), ErrorCode.UNSUPPORTED_OPERATION);
            Assert.thatUnchecked(element.getRecordTypes().stream().findFirst().isPresent(), "type filter must scan at least one table", ErrorCode.INTERNAL_ERROR);
            final var filteredType = element.getRecordTypes().stream().findFirst().get();
            if (baseTable == null) {
                baseTable = element.getRecordTypes().stream().findFirst().get();
            } else {
                Assert.thatUnchecked(baseTable.equals(filteredType), String.format("Unsupported index definition, materialized view can operate on a single table, however it is defined on more than one table '%s'", String.join(",", List.of(baseTable, filteredType))), ErrorCode.UNSUPPORTED_OPERATION);
            }
        }

        public static Pair<String, KeyExpression> evaluate(@Nonnull final RelationalExpression ref) {
            final var instance = new KeyGenerator();
            Assert.thatUnchecked(ref instanceof SelectExpression, String.format("Unsupported index definition from query '%s'", ref));
            final var keyExpression = instance.visitSelectExpression((SelectExpression) ref);
            return Pair.of(instance.getBaseTable(), keyExpression);
        }

        @Nullable
        public String getBaseTable() {
            return baseTable;
        }
    }

    @Nonnull
    static Pair<String, KeyExpression> getMaterializedViewKeyDefinition(@Nonnull final RelationalExpression relationalExpression) {
        final String invalidMatView = "invalid materialized view definition";
        Assert.thatUnchecked(relationalExpression instanceof SelectExpression, invalidMatView);
        final var selectExpression = (SelectExpression) relationalExpression;
        final var result = PlanUtils.KeyGenerator.evaluate(selectExpression);
        Assert.notNullUnchecked(result.getLeft(), String.format("could not generate a key expression from '%s'", relationalExpression));
        return Pair.of(result.getLeft(), result.getRight());
    }

    private PlanUtils() {
    }
}
