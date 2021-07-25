/*
 * OrderingProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.ValueIndexExpansionVisitor;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A property that determines whether the expression may produce duplicate entries. If the given expression is a
 * {@link RelationalExpression}, this will return whether the expression might produce multiple instances
 * of the same record. If the given expression is a {@link KeyExpression}, then this will return whether the
 * expression might return multiple results for the same record.
 */
@API(API.Status.EXPERIMENTAL)
public class OrderingProperty implements PlannerProperty<Optional<OrderingProperty.OrderingInfo>> {
    @Nonnull
    private final PlanContext context;

    private OrderingProperty(@Nonnull PlanContext context) {
        this.context = context;
    }

    @Nonnull
    @Override
    public Optional<OrderingInfo> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Optional<OrderingInfo>> childResults) {
        if (expression instanceof RecordQueryScanPlan) {
            return fromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                    ((RecordQueryScanPlan)expression).getComparisons(),
                    (RecordQueryScanPlan)expression);
        } else if (expression instanceof PrimaryScanExpression) {
            return fromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                    ((PrimaryScanExpression)expression).scanComparisons(),
                    null);
        } else if (expression instanceof RecordQueryIndexPlan || expression instanceof RecordQueryCoveringIndexPlan) {
            return fromIndexScanOrCoveringIndexScan(context, (RecordQueryPlan)expression);
        } else if (expression instanceof LogicalSortExpression) {
            final Optional<OrderingInfo> childOrderingInfoOptional = Iterables.getOnlyElement(childResults);

            final KeyExpression sortKey = ((LogicalSortExpression)expression).getSort();
            if (sortKey == null) {
                return orderingInfoFromSingleChild(expression, childResults);
            }

            final List<KeyExpression> normalizedSortKeys = sortKey.normalizeKeyForPositions();
            final ImmutableList.Builder<KeyPart> keyPartBuilder = ImmutableList.builder();
            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundKeyMap =
                    childOrderingInfoOptional.map(OrderingInfo::getEqualityBoundKeyMap).orElse(ImmutableSetMultimap.of());
            for (final KeyExpression keyExpression : normalizedSortKeys) {
                if (!equalityBoundKeyMap.containsKey(keyExpression)) {
                    keyPartBuilder.add(KeyPart.of(keyExpression, ComparisonRange.Type.EMPTY));
                }
            }

            return Optional.of(new OrderingInfo(equalityBoundKeyMap, keyPartBuilder.build(), null, null));
        } else if (expression instanceof RecordQueryIntersectionPlan) {
            final RecordQueryIntersectionPlan intersectionPlan = (RecordQueryIntersectionPlan)expression;
            return fromSetPlanAndChildren(intersectionPlan,
                    childResults,
                    orderingFromComparisonKey(intersectionPlan.getComparisonKey()),
                    Ordering::unionEqualityBoundKeys);
        } else if (expression instanceof RecordQueryUnionPlan) {
            final RecordQueryUnionPlan unionPlan = (RecordQueryUnionPlan)expression;
            return fromSetPlanAndChildren(unionPlan,
                    childResults,
                    orderingFromComparisonKey(unionPlan.getComparisonKey()),
                    Ordering::intersectEqualityBoundKeys);
        } else if (expression instanceof RecordQueryUnorderedUnionPlan) {
            return fromSetPlanAndChildren((RecordQueryUnorderedUnionPlan)expression, childResults, Ordering.preserveOrder(), Ordering::intersectEqualityBoundKeys);
        } else if (expression instanceof RecordQueryInUnionPlan) {
            return fromChildrenAndInUnionPlan(childResults, (RecordQueryInUnionPlan)expression);
        } else if (expression instanceof RecordQueryPredicatesFilterPlan) {
            return fromChildrenAndPredicatesFilterPlan(childResults, (RecordQueryPredicatesFilterPlan)expression);
        } else {
            return orderingInfoFromSingleChild(expression, childResults);
        }
    }

    @Nonnull
    private Ordering orderingFromComparisonKey(@Nonnull final KeyExpression comparisonKey) {
        return new Ordering(ImmutableSetMultimap.of(),
                comparisonKey
                        .normalizeKeyForPositions()
                        .stream()
                        .map(e -> KeyPart.of(e, ComparisonRange.Type.INEQUALITY))
                        .collect(Collectors.toList()));
    }

    private Optional<OrderingInfo> orderingInfoFromSingleChild(@Nonnull final RelationalExpression expression,
                                                               @Nonnull final List<Optional<OrderingInfo>> childResults) {
        final List<? extends Quantifier> quantifiers = expression.getQuantifiers();
        if (quantifiers.size() == 1) {
            return Iterables.getOnlyElement(childResults);
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<OrderingInfo> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Optional<OrderingInfo>> memberResults) {
        final List<Optional<Ordering>> orderingOptionals = orderingOptionals(memberResults);
        final Optional<List<KeyPart>> commonOrderingKeysOptional = Ordering.commonOrderingKeys(orderingOptionals, Ordering.preserveOrder());
        if (!commonOrderingKeysOptional.isPresent()) {
            return Optional.empty();
        }

        final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysOptional =
                Ordering.combineEqualityBoundKeys(orderingOptionals, Ordering::intersectEqualityBoundKeys);
        return commonEqualityBoundKeysOptional
                .map(keyExpressions -> new OrderingInfo(keyExpressions, commonOrderingKeysOptional.get(), null, childOrderingInfos(memberResults)));

    }

    @Nonnull
    private static List<Optional<Ordering>> orderingOptionals(@Nonnull List<Optional<OrderingInfo>> orderingInfos) {
        return orderingInfos.stream()
                .map(orderingInfoOptional -> orderingInfoOptional.map(OrderingInfo::getOrdering))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static Optional<OrderingInfo> fromIndexScanOrCoveringIndexScan(@Nonnull final PlanContext context, @Nonnull final RecordQueryPlan plan) {
        final RecordQueryIndexPlan recordQueryIndexPlan;
        if (plan instanceof RecordQueryIndexPlan) {
            recordQueryIndexPlan = (RecordQueryIndexPlan)plan;
        } else if (plan instanceof RecordQueryCoveringIndexPlan) {
            final RecordQueryPlanWithIndex planWithIndex = ((RecordQueryCoveringIndexPlan)plan).getIndexPlan();
            if (planWithIndex instanceof RecordQueryIndexPlan) {
                recordQueryIndexPlan = (RecordQueryIndexPlan)planWithIndex;
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }

        final String indexName = recordQueryIndexPlan.getIndexName();
        final RecordMetaData metaData = context.getMetaData();
        final Index index = metaData.getIndex(indexName);
        final Collection<RecordType> recordTypesForIndex = metaData.recordTypesForIndex(index);
        final KeyExpression commonPrimaryKeyForIndex = RecordMetaData.commonPrimaryKey(recordTypesForIndex);
        final KeyExpression keyExpression = ValueIndexExpansionVisitor.fullKey(index, commonPrimaryKeyForIndex);
        final ScanComparisons scanComparisons = recordQueryIndexPlan.getComparisons();
        return fromKeyAndScanComparisons(keyExpression, scanComparisons, recordQueryIndexPlan);
    }

    @Nullable
    private static List<OrderingInfo> childOrderingInfos(@Nonnull List<Optional<OrderingInfo>> memberResults) {
        List<OrderingInfo> result = new ArrayList<>(memberResults.size());
        for (Optional<OrderingInfo> member : memberResults) {
            if (!member.isPresent()) {
                return null;
            }
            result.add(member.get());
        }
        return result;
    }

    @Nonnull
    private static Optional<OrderingInfo> fromKeyAndScanComparisons(@Nullable final KeyExpression keyExpression,
                                                                    @Nonnull final ScanComparisons scanComparisons,
                                                                    @Nullable RecordQueryPlan orderedPlan) {
        if (keyExpression == null) {
            return Optional.empty();
        }
        final ImmutableSetMultimap.Builder<KeyExpression, Comparisons.Comparison> equalityBoundKeyMapBuilder = ImmutableSetMultimap.builder();
        final List<KeyExpression> normalizedKeyExpressions = keyExpression.normalizeKeyForPositions();
        final List<Comparisons.Comparison> equalityComparisons = scanComparisons.getEqualityComparisons();

        for (int i = 0; i < equalityComparisons.size(); i++) {
            final KeyExpression normalizedKeyExpression = normalizedKeyExpressions.get(i);
            final Comparisons.Comparison comparison = equalityComparisons.get(i);

            equalityBoundKeyMapBuilder.put(normalizedKeyExpression, comparison);
        }

        final ImmutableList.Builder<KeyPart> result = ImmutableList.builder();
        for (int i = scanComparisons.getEqualitySize(); i < normalizedKeyExpressions.size(); i++) {
            final KeyExpression currentKeyExpression = normalizedKeyExpressions.get(i);
            if (currentKeyExpression.createsDuplicates()) {
                break;
            }
            result.add(KeyPart.of(currentKeyExpression,
                    i == scanComparisons.getEqualitySize()
                    ? ComparisonRange.Type.INEQUALITY
                    : ComparisonRange.Type.EMPTY));
        }

        return Optional.of(new OrderingInfo(equalityBoundKeyMapBuilder.build(), result.build(), orderedPlan, null));
    }

    @Nonnull
    public static Optional<OrderingInfo> fromSetPlanAndChildren(@Nullable final RecordQuerySetPlan plan,
                                                                @Nonnull final List<Optional<OrderingInfo>> childResults,
                                                                @Nonnull final Ordering requiredOrdering,
                                                                @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparisons.Comparison>> combineFn) {
        final List<Optional<Ordering>> orderingOptionals = orderingOptionals(childResults);

        return fromOrderingsForSetPlan(orderingOptionals,
                requiredOrdering,
                combineFn)
                .map(ordering -> new OrderingInfo(ordering.getEqualityBoundKeyMap(),
                        ordering.getOrderingKeyParts(),
                        plan,
                        childOrderingInfos(childResults)));
    }

    public static Optional<Ordering> fromOrderingsForSetPlan(@Nonnull final List<Optional<Ordering>> orderingOptionals,
                                                             @Nonnull final Ordering requiredOrdering,
                                                             @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparisons.Comparison>> combineFn) {
        final Optional<List<KeyPart>> commonOrderingKeys = Ordering.commonOrderingKeys(orderingOptionals, requiredOrdering);
        if (!commonOrderingKeys.isPresent()) {
            return Optional.empty();
        }

        final Optional<SetMultimap<KeyExpression, Comparisons.Comparison>> commonEqualityBoundKeysOptional =
                Ordering.combineEqualityBoundKeys(orderingOptionals, combineFn);

        return commonEqualityBoundKeysOptional
                .map(commonEqualityBoundKeys ->
                        new Ordering(commonEqualityBoundKeys, commonOrderingKeys.get()));
    }

    public static Optional<OrderingInfo> fromChildrenAndInUnionPlan(@Nonnull final List<Optional<OrderingInfo>> orderingOptionals,
                                                                    @Nonnull final RecordQueryInUnionPlan inUnionPlan) {
        final Optional<OrderingInfo> childOrderingInfoOptional = Iterables.getOnlyElement(orderingOptionals);
        if (childOrderingInfoOptional.isPresent()) {
            final OrderingInfo childOrderingInfo = childOrderingInfoOptional.get();
            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundKeyMap = childOrderingInfo.getEqualityBoundKeyMap();
            final KeyExpression comparisonKey = inUnionPlan.getComparisonKey();

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap = HashMultimap.create(equalityBoundKeyMap);
            final ImmutableList.Builder<KeyPart> resultKeyPartBuilder = ImmutableList.builder();
            final List<KeyExpression> normalizedComparisonKeys = comparisonKey.normalizeKeyForPositions();
            for (final KeyExpression normalizedKeyExpression : normalizedComparisonKeys) {
                if (resultEqualityBoundKeyMap.containsKey(normalizedKeyExpression)) {
                    resultEqualityBoundKeyMap.removeAll(normalizedKeyExpression);
                }
                resultKeyPartBuilder.add(KeyPart.of(normalizedKeyExpression, ComparisonRange.Type.EMPTY));
            }

            return Optional.of(new OrderingInfo(resultEqualityBoundKeyMap, resultKeyPartBuilder.build(), null, null));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<OrderingInfo> fromChildrenAndPredicatesFilterPlan(@Nonnull final List<Optional<OrderingInfo>> orderingOptionals,
                                                                             @Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        final Optional<OrderingInfo> childOrderingInfoOptional = Iterables.getOnlyElement(orderingOptionals);
        if (childOrderingInfoOptional.isPresent()) {
            final OrderingInfo childOrderingInfo = childOrderingInfoOptional.get();

            final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundFileKeyExpressions =
                    predicatesFilterPlan.getPredicates()
                            .stream()
                            .flatMap(queryPredicate -> {
                                if (!(queryPredicate instanceof ValuePredicate)) {
                                    return Stream.empty();
                                }
                                final ValuePredicate valuePredicate = (ValuePredicate)queryPredicate;
                                if (!valuePredicate.getComparison().getType().isEquality()) {
                                    return Stream.empty();
                                }

                                if (!(valuePredicate.getValue() instanceof FieldValue)) {
                                    return Stream.of();
                                }
                                return Stream.of(Pair.of((FieldValue)valuePredicate.getValue(), valuePredicate.getComparison()));
                            })
                            .map(valueComparisonPair -> {

                                final FieldValue fieldValue = valueComparisonPair.getLeft();
                                final String fieldName = fieldValue.getFieldName();
                                KeyExpression keyExpression =
                                        Key.Expressions.field(fieldName);
                                final List<String> fieldPrefix = fieldValue.getFieldPrefix();
                                for (int i = fieldPrefix.size() - 1; i >= 0; i --) {
                                    keyExpression = Key.Expressions.field(fieldPrefix.get(i)).nest(keyExpression);
                                }
                                return Pair.of(keyExpression, valueComparisonPair.getRight());
                            })
                            .collect(ImmutableSetMultimap.toImmutableSetMultimap(Pair::getLeft, Pair::getRight));

            final ImmutableList<KeyPart> resultOrderingKeyParts =
                    childOrderingInfo.getOrderingKeyParts()
                            .stream()
                            .filter(keyPart -> !equalityBoundFileKeyExpressions.containsKey(keyPart.getNormalizedKeyExpression()))
                            .collect(ImmutableList.toImmutableList());

            final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap =
                    HashMultimap.create(childOrderingInfo.getEqualityBoundKeyMap());

            equalityBoundFileKeyExpressions.forEach(resultEqualityBoundKeyMap::put);

            return Optional.of(new OrderingInfo(resultEqualityBoundKeyMap, resultOrderingKeyParts, null, null));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Class to hold the info returned by this property.
     */
    public static class OrderingInfo {
        @Nonnull final Ordering ordering;
        @Nullable final RecordQueryPlan orderedPlan;
        @Nullable final List<OrderingInfo> children;

        public OrderingInfo(@Nonnull final SetMultimap<KeyExpression, Comparisons.Comparison> equalityBoundKeyMap, final List<KeyPart> orderingKeyParts,
                            @Nullable final RecordQueryPlan orderedPlan, @Nullable final List<OrderingInfo> children) {
            this.ordering = new Ordering(equalityBoundKeyMap, orderingKeyParts);
            this.orderedPlan = orderedPlan;
            this.children = children;
        }

        @Nonnull
        public Ordering getOrdering() {
            return ordering;
        }

        public SetMultimap<KeyExpression, Comparisons.Comparison> getEqualityBoundKeyMap() {
            return ordering.getEqualityBoundKeyMap();
        }

        public Set<KeyExpression> getEqualityBoundKeys() {
            return ordering.getEqualityBoundKeys();
        }

        public List<KeyPart> getOrderingKeyParts() {
            return ordering.getOrderingKeyParts();
        }

        public boolean isPreserve() {
            return ordering.isPreserve();
        }

        // TODO: This suggests that ordering and distinct should be tracked together.
        public boolean strictlyOrderedIfUnique(Function<String,Index> getIndex, int nkeys) {
            if (orderedPlan instanceof RecordQueryIndexPlan) {
                RecordQueryIndexPlan indexPlan = (RecordQueryIndexPlan)orderedPlan;
                Index index = getIndex.apply(indexPlan.getIndexName());
                return index.isUnique() && nkeys >= index.getColumnSize();
            }
            return false;
        }
    }

    public static Optional<OrderingProperty.OrderingInfo> evaluate(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull PlanContext context) {
        return ref.acceptPropertyVisitor(new OrderingProperty(context));
    }

    public static Optional<OrderingProperty.OrderingInfo> evaluate(@Nonnull RelationalExpression expression, @Nonnull PlanContext context) {
        // Won't actually be null for relational planner expressions.
        return expression.acceptPropertyVisitor(new OrderingProperty(context));
    }
}
