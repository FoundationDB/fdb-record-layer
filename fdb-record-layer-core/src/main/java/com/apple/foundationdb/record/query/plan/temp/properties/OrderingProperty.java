/*
 * CreatesDuplicatesProperty.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;

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

        final RecordMetaData metaData = context.getMetaData();

        if (expression instanceof RecordQueryScanPlan) {
            return fromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                    ((RecordQueryScanPlan)expression).getComparisons());
        } else if (expression instanceof PrimaryScanExpression) {
            return fromKeyAndScanComparisons(context.getCommonPrimaryKey(),
                    ((PrimaryScanExpression)expression).scanComparisons());
        } else if (expression instanceof RecordQueryIndexPlan) {
            final RecordQueryIndexPlan recordQueryIndexPlan = (RecordQueryIndexPlan)expression;
            final String indexName = recordQueryIndexPlan.getIndexName();
            final Index index = metaData.getIndex(indexName);
            final Collection<RecordType> recordTypesForIndex = metaData.recordTypesForIndex(index);
            final KeyExpression commonPrimaryKeyForIndex = RecordMetaData.commonPrimaryKey(recordTypesForIndex);
            final KeyExpression keyExpression = index.fullKey(commonPrimaryKeyForIndex);
            final ScanComparisons scanComparisons = recordQueryIndexPlan.getComparisons();
            return fromKeyAndScanComparisons(keyExpression, scanComparisons);
        } else if (expression instanceof IndexScanExpression) {
            final IndexScanExpression indexScanExpression = (IndexScanExpression)expression;
            if (indexScanExpression.getScanType() != IndexScanType.BY_VALUE) {
                return Optional.empty();
            }
            final String indexName = indexScanExpression.getIndexName();
            final Index index = metaData.getIndex(indexName);
            final Collection<RecordType> recordTypesForIndex = metaData.recordTypesForIndex(index);
            final KeyExpression commonPrimaryKeyForIndex = RecordMetaData.commonPrimaryKey(recordTypesForIndex);
            final KeyExpression keyExpression = index.fullKey(commonPrimaryKeyForIndex);
            final ScanComparisons scanComparisons = ((IndexScanExpression)expression).scanComparisons();
            return fromKeyAndScanComparisons(keyExpression, scanComparisons);
        } else if (expression instanceof LogicalSortExpression) {
            final Optional<OrderingInfo> childOrderingInfoOptional = Iterables.getOnlyElement(childResults);

            final KeyExpression sortKey = ((LogicalSortExpression)expression).getSort();
            final List<KeyExpression> normalizedSortKeys = sortKey.normalizeKeyForPositions();
            final ImmutableList.Builder<KeyPart> keyPartBuilder = ImmutableList.builder();
            final Set<KeyExpression> equalityBoundKeys =
                    childOrderingInfoOptional.map(OrderingInfo::getEqualityBoundKeys).orElse(ImmutableSet.of());
            for (final KeyExpression keyExpression : normalizedSortKeys) {
                if (!equalityBoundKeys.contains(keyExpression)) {
                    keyPartBuilder.add(KeyPart.of(keyExpression, ComparisonRange.Type.EMPTY));
                }
            }

            return Optional.of(new OrderingInfo(equalityBoundKeys, keyPartBuilder.build()));
        } else if (expression instanceof RecordQueryIntersectionPlan ||
                   expression instanceof LogicalIntersectionExpression) {
            final Optional<List<KeyPart>> commonOrderingKeys = commonOrderingKeys(childResults);
            if (!commonOrderingKeys.isPresent()) {
                return Optional.empty();
            }

            final Optional<Set<KeyExpression>> commonEqualityBoundKeysOptional =
                    combineEqualityBoundKeys(childResults, Sets::union);

            return commonEqualityBoundKeysOptional
                    .map(commonEqualityBoundKeys -> new OrderingInfo(commonEqualityBoundKeys, commonOrderingKeys.get()));
        } else if (expression instanceof RecordQueryUnionPlan) {
            final Optional<List<KeyPart>> commonOrderingKeys = commonOrderingKeys(childResults);
            if (!commonOrderingKeys.isPresent()) {
                return Optional.empty();
            }

            final Optional<Set<KeyExpression>> commonEqualityBoundKeysOptional =
                    combineEqualityBoundKeys(childResults, Sets::intersection);

            return commonEqualityBoundKeysOptional
                    .map(commonEqualityBoundKeys -> new OrderingInfo(commonEqualityBoundKeys, commonOrderingKeys.get()));
        } else {
            final List<? extends Quantifier> quantifiers = expression.getQuantifiers();
            if (quantifiers.size() == 1) {
                return Iterables.getOnlyElement(childResults);
            }
            return Optional.empty();
        }
    }

    @Nonnull
    @Override
    public Optional<OrderingInfo> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Optional<OrderingInfo>> memberResults) {
        final Optional<List<KeyPart>> commonOrderingKeysOptional = commonOrderingKeys(memberResults);
        if (!commonOrderingKeysOptional.isPresent()) {
            return Optional.empty();
        }

        final Optional<Set<KeyExpression>> commonEqualityBoundKeysOptional =
                combineEqualityBoundKeys(memberResults, Sets::intersection);
        return commonEqualityBoundKeysOptional
                .map(keyExpressions -> new OrderingInfo(keyExpressions, commonOrderingKeysOptional.get()));

    }

    @Nonnull
    private static Optional<List<KeyPart>> commonOrderingKeys(@Nonnull List<Optional<OrderingInfo>> orderingInfoOptionals) {
        final Iterator<Optional<OrderingInfo>> membersIterator = orderingInfoOptionals.iterator();
        if (!membersIterator.hasNext()) {
            // don't bail on incorrect graph structure, just return empty()
            return Optional.empty();
        }

        final Optional<OrderingInfo> commonOrderingInfoOptional = membersIterator.next();
        if (!commonOrderingInfoOptional.isPresent()) {
            return Optional.empty();
        }

        final OrderingInfo commonOrderingInfo = commonOrderingInfoOptional.get();
        List<KeyPart> commonOrderingKeys = commonOrderingInfo.getOrderingKeyParts();

        while (membersIterator.hasNext()) {
            final Optional<OrderingInfo> currentOrderingInfoOptional = membersIterator.next();
            if (!currentOrderingInfoOptional.isPresent()) {
                return Optional.empty();
            }

            final OrderingInfo currentOrderingInfo = currentOrderingInfoOptional.get();
            final List<KeyPart> currentOrderingKeys = currentOrderingInfo.getOrderingKeyParts();

            // special case -- if both are empty (and if one is -- both should be), the result is empty
            if (commonOrderingKeys.isEmpty() && currentOrderingKeys.isEmpty()) {
                continue;
            }

            if (commonOrderingKeys.isEmpty()) {
                // current is not empty
                return Optional.empty();
            }

            for (int i = 0; i < commonOrderingKeys.size(); i++) {
                final KeyPart commonKeyPart = commonOrderingKeys.get(i);

                if (i == currentOrderingKeys.size()) {
                    commonOrderingKeys = KeyPart.prefix(commonOrderingKeys, i);
                    break;
                }
                final KeyPart currentKeyPart = currentOrderingKeys.get(i);

                Verify.verify(currentKeyPart.getComparisonRangeType() == ComparisonRange.Type.INEQUALITY ||
                              currentKeyPart.getComparisonRangeType() == ComparisonRange.Type.EMPTY);
                if (!commonKeyPart.getNormalizedKeyExpression().equals(currentKeyPart.getNormalizedKeyExpression())) {
                    commonOrderingKeys = KeyPart.prefix(commonOrderingKeys, i);
                    break;
                }
            }

            if (commonOrderingKeys.isEmpty()) {
                return Optional.empty();
            }
        }

        return Optional.of(commonOrderingKeys);
    }

    @Nonnull
    private Optional<Set<KeyExpression>> combineEqualityBoundKeys(@Nonnull final List<Optional<OrderingInfo>> orderingInfoOptionals,
                                                                  @Nonnull final BinaryOperator<Set<KeyExpression>> combineFn) {
        final Iterator<Optional<OrderingInfo>> membersIterator = orderingInfoOptionals.iterator();
        if (!membersIterator.hasNext()) {
            // don't bail on incorrect graph structure, just return empty()
            return Optional.empty();
        }

        final Optional<OrderingInfo> commonOrderingInfoOptional = membersIterator.next();
        if (!commonOrderingInfoOptional.isPresent()) {
            return Optional.empty();
        }

        final OrderingInfo commonOrderingInfo = commonOrderingInfoOptional.get();
        Set<KeyExpression> commonEqualityBoundKeys = commonOrderingInfo.getEqualityBoundKeys();

        while (membersIterator.hasNext()) {
            final Optional<OrderingInfo> currentOrderingInfoOptional = membersIterator.next();
            if (!currentOrderingInfoOptional.isPresent()) {
                return Optional.empty();
            }

            final OrderingInfo currentOrderingInfo = currentOrderingInfoOptional.get();

            final Set<KeyExpression> currentEqualityBoundKeys = currentOrderingInfo.getEqualityBoundKeys();
            commonEqualityBoundKeys = combineFn.apply(commonEqualityBoundKeys, currentEqualityBoundKeys);
        }

        return Optional.of(commonEqualityBoundKeys);
    }

    @Nonnull
    private static Optional<OrderingInfo> fromKeyAndScanComparisons(@Nullable final KeyExpression keyExpression,
                                                                    @Nonnull final ScanComparisons scanComparisons) {
        if (keyExpression == null) {
            return Optional.empty();
        }
        final List<KeyExpression> normalizedKeyExpressions = keyExpression.normalizeKeyForPositions();
        final Set<KeyExpression> equalityBoundKeyExpressions = ImmutableSet.copyOf(normalizedKeyExpressions.subList(0, scanComparisons.getEqualitySize()));

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

        return Optional.of(new OrderingInfo(equalityBoundKeyExpressions, result.build()));
    }

    /**
     * Class to hold the info returned by this property.
     */
    public static class OrderingInfo {
        final Set<KeyExpression> equalityBoundKeys;
        final List<KeyPart> orderingKeyParts;

        public OrderingInfo(final Set<KeyExpression> equalityBoundKeys, final List<KeyPart> orderingKeyParts) {
            this.orderingKeyParts = ImmutableList.copyOf(orderingKeyParts);
            this.equalityBoundKeys = ImmutableSet.copyOf(equalityBoundKeys);
        }

        public Set<KeyExpression> getEqualityBoundKeys() {
            return equalityBoundKeys;
        }

        public List<KeyPart> getOrderingKeyParts() {
            return orderingKeyParts;
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
