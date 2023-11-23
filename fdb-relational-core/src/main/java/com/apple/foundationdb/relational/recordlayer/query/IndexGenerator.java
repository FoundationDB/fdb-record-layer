/*
 * IndexGenerator.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IndexPredicateExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Generates a {@link KeyExpression} from a given query plan.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public final class IndexGenerator {

    /**
     * Map from each correlation in the query plan to its list of results.
     */
    @Nonnull
    private final IdentityHashMap<CorrelationIdentifier, Value> correlatedKeyExpressions = new IdentityHashMap<>();

    @Nonnull
    private final List<RelationalExpression> relationalExpressions;

    @Nonnull
    private final RelationalExpression relationalExpression;

    private IndexGenerator(@Nonnull final RelationalExpression relationalExpression) {
        collectQuantifiers(relationalExpression);
        final var partialOrder = ReferencesAndDependenciesProperty.evaluate(GroupExpressionRef.of(relationalExpression));
        relationalExpressions =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RelationalException("graph has cycles", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException())
                        .stream()
                        .map(ExpressionRef::get)
                        .collect(toList());
        this.relationalExpression = relationalExpression;
    }

    @Nonnull
    public RecordLayerIndex generate(@Nonnull final String indexName, boolean isUnique, @Nonnull final Type.Record tableType, boolean containsNonNullableArray) {
        final var indexBuilder = RecordLayerIndex.newBuilder()
                .setName(indexName)
                .setTableName(getRecordTypeName())
                .setUnique(isUnique);

        collectQuantifiers(relationalExpression);

        final var partialOrder = ReferencesAndDependenciesProperty.evaluate(GroupExpressionRef.of(relationalExpression));
        final var expressionRefs =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles")).stream().map(ExpressionRef::get).collect(toList());

        checkValidity(expressionRefs);

        // add predicates
        final var predicate = getTopLevelPredicate(Lists.reverse(expressionRefs));
        if (predicate != null) {
            indexBuilder.setPredicate(IndexPredicate.fromQueryPredicate(predicate).toProto());
        }

        final var simplifiedValues = collectResultValues(relationalExpression.getResultValue());

        final var unsupportedAggregates = simplifiedValues.stream().filter(sv -> sv instanceof StreamableAggregateValue && !(sv instanceof IndexableAggregateValue)).collect(toList());
        Assert.thatUnchecked(unsupportedAggregates.isEmpty(), String.format("Unsupported aggregate index definition containing non-indexable aggregation (%s), consider using a value index on the aggregated column instead.",
                unsupportedAggregates.stream().map(Objects::toString).collect(joining(","))), ErrorCode.UNSUPPORTED_OPERATION);

        Assert.thatUnchecked(simplifiedValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof IndexableAggregateValue || sv instanceof VersionValue));
        final var aggregateValues = simplifiedValues.stream().filter(sv -> sv instanceof IndexableAggregateValue).collect(toList());
        final var fieldValues = simplifiedValues.stream().filter(sv -> !(sv instanceof IndexableAggregateValue)).collect(toList());
        final var versionValues = simplifiedValues.stream().filter(sv -> sv instanceof VersionValue).map(sv -> (VersionValue) sv).collect(toList());
        Assert.thatUnchecked(versionValues.size() <= 1, "Cannot have index with more than one version column", ErrorCode.UNSUPPORTED_OPERATION);
        final var orderByValues = getOrderByValues(relationalExpression);
        if (aggregateValues.isEmpty()) {
            indexBuilder.setIndexType(versionValues.isEmpty() ? IndexTypes.VALUE : IndexTypes.VERSION);
            Assert.thatUnchecked(orderByValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof VersionValue), "Unsupported index definition, order by must be a subset of projection list", ErrorCode.UNSUPPORTED_OPERATION);
            if (fieldValues.size() > 1) {
                Assert.thatUnchecked(!orderByValues.isEmpty(), "Unsupported index definition, value indexes must have an order by clause at the top level", ErrorCode.UNSUPPORTED_OPERATION);
            }
            final var reordered = reorderValues(fieldValues, orderByValues);
            final var expression = generate(reordered);
            final var splitPoint = orderByValues.isEmpty() ? -1 : orderByValues.size();
            if (splitPoint != -1 && splitPoint < fieldValues.size()) {
                indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(keyWithValue(expression, splitPoint).toKeyExpression(), tableType, containsNonNullableArray)));
            } else {
                indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(expression.toKeyExpression(), tableType, containsNonNullableArray)));
            }
        } else {
            Assert.thatUnchecked(aggregateValues.size() == 1, "Unsupported index definition, multiple group by aggregations found", ErrorCode.UNSUPPORTED_OPERATION);
            if (!orderByValues.isEmpty() && !orderByValues.containsAll(fieldValues)) {
                Assert.failUnchecked("Unsupported index definition, attempt to create a covering aggregate index", ErrorCode.UNSUPPORTED_OPERATION);
            }
            final var aggregateValue = (AggregateValue) aggregateValues.get(0);
            final Optional<KeyExpression> groupingKeyExpression = fieldValues.isEmpty() ? Optional.empty() : Optional.of(generate(fieldValues));
            final var indexExpressionAndType = generateAggregateIndexKeyExpression(aggregateValue, groupingKeyExpression);
            indexBuilder.setIndexType(indexExpressionAndType.getRight());
            indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(indexExpressionAndType.getLeft().toKeyExpression(), tableType, containsNonNullableArray)));
        }
        return indexBuilder.build();
    }

    @Nonnull
    private List<Value> collectResultValues(@Nonnull final Value value) {
        final var resultValue = simplify(value);
        // if the final result value contains nothing but the aggregation value, add the grouping values to it.
        final var isSingleAggregation = resultValue.size() == 1 && resultValue.get(0) instanceof IndexableAggregateValue;
        final var maybeGroupBy = relationalExpressions.stream().filter(exp -> exp instanceof GroupByExpression).findFirst();
        if (maybeGroupBy.isPresent() && isSingleAggregation) {
            final var groupBy = maybeGroupBy.get();
            final var groupingValues = ((GroupByExpression) groupBy).getGroupingValue();
            if (groupingValues == null) {
                return resultValue;
            } else {
                final var simplifiedGroupingValues = Values.deconstructRecord(groupingValues).stream().map(this::dereference).map(v -> v.simplify(AliasMap.emptyMap(), Set.of()));
                return Stream.concat(resultValue.stream(), simplifiedGroupingValues).collect(toList());
            }
        } else {
            return resultValue;
        }
    }

    @Nonnull
    private List<Value> simplify(@Nonnull final Value value) {
        return Values.deconstructRecord(value)
                .stream()
                .map(this::dereference)
                .map(v -> v.simplify(AliasMap.emptyMap(), Set.of()))
                .collect(toList());
    }

    @Nonnull
    private List<Value> getOrderByValues(@Nonnull final RelationalExpression relationalExpression) {
        if (relationalExpression instanceof LogicalSortExpression) {
            final var logicalSortExpression = (LogicalSortExpression) relationalExpression;
            final var reverseAliasMap = AliasMap.of(Quantifier.current(), logicalSortExpression.getQuantifiers().get(0).getAlias());
            return logicalSortExpression.getSortValues()
                    .stream()
                    .flatMap(v -> v.getResultType().getTypeCode() == Type.TypeCode.RECORD ? Values.deconstructRecord(v).stream() : Stream.of(v))
                    .map(v -> v.rebase(reverseAliasMap))
                    .map(this::dereference)
                    .map(v -> v.simplify(AliasMap.emptyMap(), Set.of()))
                    .collect(ImmutableList.toImmutableList());
        }
        return List.of();
    }

    private static List<Value> reorderValues(@Nonnull final List<Value> values, @Nonnull List<Value> orderByValues) {
        Assert.thatUnchecked(values.size() >= orderByValues.size());
        if (orderByValues.isEmpty()) {
            return values;
        }
        final var remaining = values.stream().filter(v -> !orderByValues.contains(v)).collect(ImmutableList.toImmutableList());
        return ImmutableList.<Value>builder().addAll(orderByValues).addAll(remaining).build();
    }

    @Nonnull
    private Pair<KeyExpression, String> generateAggregateIndexKeyExpression(@Nonnull final AggregateValue aggregateValue, @Nonnull final Optional<KeyExpression> maybeGroupingExpression) {
        Assert.thatUnchecked(aggregateValue instanceof IndexableAggregateValue);
        final var child = Iterables.getOnlyElement(aggregateValue.getChildren());
        final KeyExpression groupedValue;
        final GroupingKeyExpression keyExpression;
        if (aggregateValue instanceof CountValue && child instanceof RecordConstructorValue && ((RecordConstructorValue) child).getColumns().isEmpty()) {
            if (maybeGroupingExpression.isPresent()) {
                keyExpression = new GroupingKeyExpression(maybeGroupingExpression.get(), 0);
            } else {
                keyExpression = new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0);
            }
        } else {
            Assert.thatUnchecked(child instanceof FieldValue, "Unsupported index definition, expecting a column argument in aggregation function");
            groupedValue = generate(List.of(child));
            Assert.thatUnchecked(groupedValue instanceof FieldKeyExpression || groupedValue instanceof ThenKeyExpression);
            if (maybeGroupingExpression.isPresent()) {
                keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                        ((FieldKeyExpression) groupedValue).groupBy(maybeGroupingExpression.get()) :
                        ((ThenKeyExpression) groupedValue).groupBy(maybeGroupingExpression.get());
            } else {
                keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                        ((FieldKeyExpression) groupedValue).ungrouped() :
                        ((ThenKeyExpression) groupedValue).ungrouped();
            }
        }
        return Pair.of(keyExpression, ((IndexableAggregateValue) aggregateValue).getIndexName());
    }

    @Nonnull
    private KeyExpression generate(@Nonnull final List<Value> fields) {
        if (fields.isEmpty()) {
            return EmptyKeyExpression.EMPTY;
        } else if (fields.size() == 1) {
            return toKeyExpression(fields.get(0));
        }

        List<FieldValueTrieNode> trieNodes = new ArrayList<>(fields.size());
        List<KeyExpression> components = new ArrayList<>(fields.size());
        PeekingIterator<Value> valueIterator = Iterators.peekingIterator(fields.iterator());
        while (valueIterator.hasNext()) {
            if (!(valueIterator.peek() instanceof FieldValue)) {
                components.add(toKeyExpression(valueIterator.next()));
            } else {
                FieldValueTrieNode trieNode = FieldValueTrieNode.computeTrieForValues(FieldValue.FieldPath.empty(), valueIterator);
                trieNode.validateNoOverlaps(trieNodes);
                trieNodes.add(trieNode);

                components.add(toKeyExpression(trieNode));
            }
        }

        if (components.size() == 1) {
            return components.get(0);
        } else {
            return concat(components);
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(Value value) {
        if (value instanceof VersionValue) {
            return VersionKeyExpression.VERSION;
        } else if (value instanceof FieldValue) {
            FieldValue fieldValue = (FieldValue) value;
            return toKeyExpression(fieldValue.getFieldPath().getFieldAccessors().stream().map(acc -> Pair.of(acc.getName(), acc.getType())).collect(toList()));
        } else {
            Assert.failUnchecked("unable to construct expression", ErrorCode.UNSUPPORTED_OPERATION);
            return null;
        }
    }

    @Nonnull
    private static KeyExpression toKeyExpression(@Nonnull final FieldValueTrieNode trieNode) {
        Assert.notNullUnchecked(trieNode.getChildrenMap());
        Assert.thatUnchecked(!trieNode.getChildrenMap().isEmpty());

        final var childrenMap = trieNode.getChildrenMap();
        final var exprConstituents = childrenMap.keySet().stream().map(key -> {
            var expr = toKeyExpression(Objects.requireNonNull(key.getName()), key.getType());
            final var value = childrenMap.get(key);
            if (value.getChildrenMap() != null) {
                return expr.nest(toKeyExpression(value));
            } else {
                return expr;
            }
        }).map(v -> (KeyExpression) v).collect(toList());
        if (exprConstituents.size() == 1) {
            return exprConstituents.get(0);
        } else {
            return concat(exprConstituents);
        }
    }

    private void checkValidity(@Nonnull final List<? extends RelationalExpression> expressionRefs) {

        // there must be exactly one type full-unordered-scan, no joins, no self-joins.
        final var numScans = expressionRefs.stream().filter(r -> r instanceof FullUnorderedScanExpression).count();
        Assert.thatUnchecked(numScans == 1, String.format("Unsupported index definition, %s iteration generator found", numScans == 0 ? "no" : "more than one"), ErrorCode.UNSUPPORTED_OPERATION);

        // there must be at most a single group by
        final var numGroupBy = expressionRefs.stream().filter(r -> r instanceof GroupByExpression).count();
        Assert.thatUnchecked(numGroupBy <= 1, "Unsupported index definition, multiple group by expressions found", ErrorCode.UNSUPPORTED_OPERATION);

        // there can be only one aggregation in group by expression (maybe we can relax this in the future).
        final var groupByContainsOneAggregation = expressionRefs.stream().filter(r -> r instanceof GroupByExpression).map(r -> (GroupByExpression) r).noneMatch(g -> Values.deconstructRecord(g.getAggregateValue()).size() > 1);
        Assert.thatUnchecked(groupByContainsOneAggregation, "Unsupported index definition, found group by expression with more than one aggregation", ErrorCode.UNSUPPORTED_OPERATION);

        // result values of each operation must be simple, e.g. no arithmetic values.
        final var allRecordValues = expressionRefs.stream().allMatch(r -> (r.getResultValue().getResultType().getTypeCode() == Type.TypeCode.RECORD));
        Assert.thatUnchecked(allRecordValues, "Unsupported index definition, some operators return non-record values", ErrorCode.UNSUPPORTED_OPERATION);

        final var allSimpleValues = expressionRefs.stream().allMatch(r -> Values.deconstructRecord(r.getResultValue()).stream().allMatch(v -> v instanceof FieldValue || v instanceof VersionValue || v instanceof QuantifiedObjectValue || v instanceof AggregateValue));
        Assert.thatUnchecked(allSimpleValues, "Unsupported index definition, not all fields can be mapped to key expression in", ErrorCode.UNSUPPORTED_OPERATION);
    }

    @Nullable
    private static QueryPredicate getTopLevelPredicate(@Nonnull final List<? extends RelationalExpression> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }
        int currentExpression = 0;
        if (expressions.get(currentExpression) instanceof LogicalSortExpression) {
            currentExpression++;
        }
        if (expressions.size() > currentExpression && expressions.get(currentExpression) instanceof SelectExpression) {
            if (expressions.size() > (currentExpression + 1) && expressions.get(currentExpression + 1) instanceof GroupByExpression) {
                // the above select-having must not contain any predicate.
                Assert.thatUnchecked(((SelectExpression) expressions.get(currentExpression)).getPredicates().isEmpty(), "Unsupported index definition, found predicate in select-having", ErrorCode.UNSUPPORTED_OPERATION);
                currentExpression++; // group-by expression.
                Assert.thatUnchecked(expressions.size() > currentExpression);
                currentExpression++; // select-where.
            }
        }
        // current expression is either top-level select, or select-where or top-level group by.
        // make sure any other select statement does not have any predicates defined.
        for (int i = currentExpression + 1; i < expressions.size(); i++) {
            if (expressions.get(i) instanceof SelectExpression) {
                final var innerSelect = (SelectExpression) expressions.get(i);
                Assert.thatUnchecked(innerSelect.getPredicates().isEmpty(), "Unsupported index definition, found predicate in inner-select", ErrorCode.UNSUPPORTED_OPERATION);
            }
        }
        final var predicates = ((SelectExpression) expressions.get(currentExpression)).getPredicates().stream().map(QueryPredicate::toResidualPredicate).collect(toList());
        // todo (yhatem) make sure we through if the generated DNF does not meet the deserialization requirements.
        if (predicates.isEmpty()) {
            return null;
        }
        final var conjunction = predicates.size() == 1 ? predicates.get(0) : AndPredicate.and(predicates);
        final var result = BooleanPredicateNormalizer.getDefaultInstanceForDnf().normalize(conjunction, false).orElse(conjunction);
        Assert.thatUnchecked(IndexPredicate.isSupported(result), String.format("Unsupported predicate '%s'", result));
        if (IndexPredicateExpansion.dnfPredicateToRanges(result).isEmpty()) {
            return conjunction;
        }
        return result;
    }

    private static final class AnnotatedAccessor extends FieldValue.ResolvedAccessor {

        private final int marker;

        private AnnotatedAccessor(@Nonnull Type fieldType,
                                  @Nullable String fieldName,
                                  int fieldOrdinal,
                                  int marker) {
            super(fieldName, fieldOrdinal, fieldType);
            this.marker = marker;
        }

        @Nonnull
        public static AnnotatedAccessor of(@Nonnull FieldValue.ResolvedAccessor resolvedAccessor, int marker) {
            return new AnnotatedAccessor(resolvedAccessor.getType(), resolvedAccessor.getName(), resolvedAccessor.getOrdinal(), marker);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            AnnotatedAccessor that = (AnnotatedAccessor) o;
            return marker == that.marker;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), marker);
        }
    }

    private void collectQuantifiers(@Nonnull final RelationalExpression relationalExpression) {
        MutableInt counter = new MutableInt(0);
        collectQuantifiersInternal(relationalExpression, counter);
    }

    private void collectQuantifiersInternal(@Nonnull final RelationalExpression relationalExpression, @Nonnull MutableInt explodeCounter) {
        for (final var qun : relationalExpression.getQuantifiers()) {
            if (qun.getRangesOver().get() instanceof ExplodeExpression) {
                explodeCounter.increment();
                final var collectionValue = ((ExplodeExpression) qun.getRangesOver().get()).getCollectionValue();
                if (collectionValue instanceof FieldValue) {
                    final var field = (FieldValue) collectionValue;
                    final var fieldAccessors = new ArrayList<>(field.getFieldPath().getFieldAccessors());
                    fieldAccessors.set(fieldAccessors.size() - 1, AnnotatedAccessor.of(fieldAccessors.get(fieldAccessors.size() - 1), explodeCounter.getValue()));
                    correlatedKeyExpressions.put(qun.getAlias(), FieldValue.ofFields(((FieldValue) collectionValue).getChild(), new FieldValue.FieldPath(fieldAccessors)));
                } else {
                    correlatedKeyExpressions.put(qun.getAlias(), collectionValue);
                }
            } else {
                correlatedKeyExpressions.put(qun.getAlias(), qun.getRangesOver().get().getResultValue());
            }
            collectQuantifiersInternal(qun.getRangesOver().get(), explodeCounter);
        }
    }

    @Nonnull
    private Value dereference(@Nonnull final Value value) {
        if (value instanceof RecordConstructorValue) {
            return RecordConstructorValue.ofColumns(
                    ((RecordConstructorValue) value).getColumns()
                            .stream()
                            .map(c -> Column.of(c.getField(), dereference(c.getValue())))
                            .collect(toList()));
        } else if (value instanceof CountValue) {
            final var children = StreamSupport.stream(value.getChildren().spliterator(), false).collect(toList());
            Verify.verify(children.size() <= 1);
            if (!children.isEmpty()) {
                return value.withChildren(Collections.singleton(dereference(children.get(0))));
            } else {
                return value;
            }
        } else if (value instanceof FieldValue || value instanceof IndexableAggregateValue) {
            final var valueWithChild = (ValueWithChild) value;
            return valueWithChild.withNewChild(dereference(valueWithChild.getChild()));
        } else if (value instanceof QuantifiedObjectValue) {
            return dereference(correlatedKeyExpressions.get(value.getCorrelatedTo().stream().findFirst().orElseThrow()));
        } else {
            return value;
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull final List<Pair<String, Type>> fields) {
        return toKeyExpression(fields, 0);
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull final List<Pair<String, Type>> fields, int index) {
        Assert.thatUnchecked(!fields.isEmpty());
        final var field = fields.get(index);
        final var keyExpression = toKeyExpression(field.getLeft(), field.getRight());
        if (index + 1 < fields.size()) {
            return keyExpression.nest(toKeyExpression(fields, index + 1));
        }
        return keyExpression;
    }

    @Nonnull
    public String getRecordTypeName() {
        final var expressionRefs = relationalExpressions.stream()
                .filter(r -> r instanceof LogicalTypeFilterExpression)
                .map(r -> (LogicalTypeFilterExpression) r)
                .collect(toList());
        Assert.thatUnchecked(expressionRefs.size() == 1, "Unsupported query, expected to find exactly one type filter operator", ErrorCode.UNSUPPORTED_OPERATION);
        final var recordTypes = expressionRefs.get(0).getRecordTypes();
        Assert.thatUnchecked(recordTypes.size() == 1, String.format("Unsupported query, expected to find exactly one record type in type filter operator, however found %s", recordTypes.isEmpty() ? "nothing" : String.join(",", recordTypes)));
        return recordTypes.stream().findFirst().orElseThrow();
    }

    @Nonnull
    private static FieldKeyExpression toKeyExpression(@Nonnull final String name, @Nonnull final Type type) {
        Assert.notNullUnchecked(name);
        final var fanType = type.getTypeCode() == Type.TypeCode.ARRAY ?
                KeyExpression.FanType.FanOut :
                KeyExpression.FanType.None;
        return field(name, fanType);
    }

    @Nonnull
    public static IndexGenerator from(@Nonnull final RelationalExpression relationalExpression) {
        return new IndexGenerator(relationalExpression);
    }
}
