/*
 * PlanOrderingKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithChild;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The keys that order the results from a plan.
 *
 * Given a record type with primary key <code>k1, k2</code> and an index on <code>k1, v1, v2</code>,
 * the index entries are <code>k1, v1, v2, k2</code>.
 *
 * Given a scan on this index for <code>k1 = ? AND v1 = ?</code>, the results are dictionary ordered by <code>v2, k2</code>.
 *
 * Up to <code>prefixSize</code>, <code>2</code> in this case, the ordering is trivial: all records have the same value.
 * <code>primaryKeyStart</code> is the earlier position for part of the primary key; in this case, <code>0</code>.
 * <code>primaryKeyTail</code> is the position after which only primary key fields appear; in this case, <code>3</code>.
 */
@API(API.Status.INTERNAL)
public class PlanOrderingKey {
    @Nonnull
    private final List<KeyExpression> keys;
    private final int prefixSize;
    private final int primaryKeyStart;
    private final int primaryKeyTail;
    // Keep a set of positions that are equality bound but are not in the prefix. (All components in the prefix are equality bound.)
    @Nonnull
    private final Set<Integer> equalityBoundInTail;

    public PlanOrderingKey(@Nonnull List<KeyExpression> keys, int prefixSize,
                           int primaryKeyStart, int primaryKeyTail) {
        this.keys = keys;
        this.prefixSize = prefixSize;
        this.primaryKeyStart = primaryKeyStart;
        this.primaryKeyTail = primaryKeyTail;

        // Look through the components in the (non-equality bound) suffix of the
        // plan ordering key. If any key appears first in the equality-bound prefix,
        // make a note of that so that we can skip over that key when trying to match
        // a key against the next non-equality bound component during ordering matching
        ImmutableSet.Builder<Integer> equalityBoundBuilder = ImmutableSet.builder();
        for (int pos = prefixSize; pos < keys.size(); pos++) {
            final KeyExpression component = keys.get(pos);
            int firstIndex = keys.indexOf(component);
            if (firstIndex >= 0 && firstIndex < prefixSize) {
                // Note: firstIndex should always be greater than or equal to 0, because
                // the index should be at least equal to pos. However, just to
                // guard against .equals() not working, require that firstIndex >= 0 before
                // adding the position to the set
                equalityBoundBuilder.add(pos);
            }
        }
        this.equalityBoundInTail = equalityBoundBuilder.build();
    }

    @Nonnull
    public List<KeyExpression> getKeys() {
        return keys;
    }

    public int getPrefixSize() {
        return prefixSize;
    }

    public int getSuffixSize() {
        return keys.size() - prefixSize;
    }

    public int getPrimaryKeyStart() {
        return primaryKeyStart;
    }

    public int getPrimaryKeyTail() {
        return primaryKeyTail;
    }

    /**
     * Return <code>true</code> if only primary key fields follow the prefix.
     * If so, all the (non-trivial) ordering of results is by primary key.
     * @return <code>true</code> if this ordering key is ordered by primary key
     */
    public boolean isPrimaryKeyOrdered() {
        return prefixSize >= primaryKeyTail;
    }

    private boolean isEqualityBound(int componentIndex) {
        return componentIndex < prefixSize || equalityBoundInTail.contains(componentIndex);
    }

    @Nullable
    public static PlanOrderingKey forPlan(@Nonnull RecordMetaData metaData, @Nonnull RecordQueryPlan queryPlan,
                                          @Nullable KeyExpression primaryKey) {
        if (primaryKey == null) {
            return null;
        }
        
        while (queryPlan instanceof RecordQueryPlanWithChild) {
            // as long as we can tunnel through single-child plans
            if (queryPlan instanceof RecordQueryFilterPlan ||
                    queryPlan instanceof RecordQueryTypeFilterPlan) {
                // if we know the kind of plan does not modify the ordered-ness
                queryPlan = ((RecordQueryPlanWithChild)queryPlan).getChild();
            } else {
                break;
            }
        }
        if (queryPlan instanceof PlanWithOrderingKey) {
            return ((PlanWithOrderingKey)queryPlan).getPlanOrderingKey();
        }
        if (queryPlan instanceof RecordQueryPlanWithIndex) {
            final RecordQueryPlanWithIndex indexPlan = (RecordQueryPlanWithIndex)queryPlan;
            final Index index = metaData.getIndex(indexPlan.getIndexName());
            final List<KeyExpression> keys = new ArrayList<>(index.getRootExpression().normalizeKeyForPositions());
            int pkeyStart = keys.size();
            int pKeyTail = pkeyStart;
            // Primary keys come after index value keys, unless they were already part of it.
            for (KeyExpression pkey : primaryKey.normalizeKeyForPositions()) {
                int pos = keys.indexOf(pkey);
                if (pos < 0) {
                    keys.add(pkey);
                } else if (pkeyStart > pos) {
                    pkeyStart = pos;
                }
            }
            final int prefixSize;
            if (indexPlan instanceof RecordQueryIndexPlan) {
                if (IndexTypes.MULTIDIMENSIONAL.equals(index.getType())) {
                    return null;
                }
                if (!((RecordQueryIndexPlan)indexPlan).hasScanComparisons()) {
                    return null;
                }
                prefixSize = ((RecordQueryIndexPlan)indexPlan).getScanComparisons().getEqualitySize();
            } else if (indexPlan instanceof RecordQueryTextIndexPlan) {
                final TextScan textScan = ((RecordQueryTextIndexPlan)indexPlan).getTextScan();
                int groupingSize = textScan.getGroupingComparisons() != null ? textScan.getGroupingComparisons().getEqualitySize() : 0;
                int suffixSize = textScan.getSuffixComparisons() != null ? textScan.getSuffixComparisons().getEqualitySize() : 0;
                if (textScan.getTextComparison().getType().isEquality()) {
                    // Can use the equality comparisons in the grouping columns and any columns after the text index
                    // plus the text column itself.
                    prefixSize = groupingSize + suffixSize + 1;
                } else {
                    // The inequality text comparisons do not really allow for combining results as they are ordered
                    // by the token that the prefix scan just so happened to hit.
                    return null;
                }
            } else {
                // Some unknown index plan. Maybe this should throw an error?
                return null;
            }
            return new PlanOrderingKey(keys, prefixSize, pkeyStart, pKeyTail);
        } else if (queryPlan instanceof RecordQueryScanPlan) {
            final RecordQueryScanPlan scanPlan = (RecordQueryScanPlan)queryPlan;
            return new PlanOrderingKey(primaryKey.normalizeKeyForPositions(),
                                       scanPlan.getScanComparisons().getEqualitySize(),
                                       0, 0);
        } else if (queryPlan instanceof RecordQueryIntersectionOnKeyExpressionPlan) {
            return forComparisonKey(((RecordQueryIntersectionOnKeyExpressionPlan)queryPlan).getComparisonKeyExpression(), primaryKey);
        } else if (queryPlan instanceof RecordQueryUnionOnKeyExpressionPlan) {
            return forComparisonKey(((RecordQueryUnionOnKeyExpressionPlan)queryPlan).getComparisonKeyExpression(), primaryKey);
        } else {
            return null;
        }
    }

    @Nonnull
    public static PlanOrderingKey forComparisonKey(@Nonnull KeyExpression comparisonKey, @Nonnull KeyExpression primaryKey) {
        final List<KeyExpression> keys = comparisonKey.normalizeKeyForPositions();
        final List<KeyExpression> pkeys = primaryKey.normalizeKeyForPositions();
        int firstPrimaryKeyPosition = -1;
        int lastNonPrimaryKeyPosition = -1;
        for (int i = 0; i < keys.size(); i++) {
            if (pkeys.contains(keys.get(i))) {
                if (firstPrimaryKeyPosition < 0) {
                    firstPrimaryKeyPosition = i;
                }
            } else {
                lastNonPrimaryKeyPosition = i;
            }
        }
        return new PlanOrderingKey(keys, 0, firstPrimaryKeyPosition, lastNonPrimaryKeyPosition + 1);
    }

    /**
     * Get a key to be evaluated on record results to drive a merge. This key must respect the underlying ordering
     * so that only a single record needs to be buffered from each cursor. And it must uniquely identify duplicates
     * by including the primary key.
     * @param plans the plans for which to find a compatible ordering, all of which must have ordering keys
     * @param candidateKey a suggested key to use
     * @param candidateOnly only accept the given candidate
     * @return a comparison key compatible with all the plans
     */
    @Nullable
    @SuppressWarnings("PMD.UnusedAssignment") // confused by break?
    public static KeyExpression mergedComparisonKey(@Nonnull List<RecordQueryPlanner.ScoredPlan> plans,
                                                    @Nullable KeyExpression candidateKey,
                                                    boolean candidateOnly) {
        if (candidateOnly) {
            if (candidateKey == null) {
                return null;
            }
            // See whether candidateKey, the only one allowed or one sufficient for one of the plans, is good enough for all plans.
            for (RecordQueryPlanner.ScoredPlan plan : plans) {
                final PlanOrderingKey planOrderingKey = Objects.requireNonNull(plan.planOrderingKey);
                if (!isOrderingCompatible(planOrderingKey, candidateKey)) {
                    return null;
                }
            }
            return candidateKey;
        } else {
            // Sort plans descending by the number of non-equality bound ordering keys. Keys with shorter suffixes
            // only need to match a subset of the ordering keys of the longest one (if the missing elements are
            // equality bound within the plans with shorter suffixes)
            List<RecordQueryPlanner.ScoredPlan> plansDescendingBySuffixSize = plans.stream()
                    .sorted((p1, p2) -> -1 * Integer.compare(p1.planOrderingKey.getSuffixSize(), p2.planOrderingKey.getSuffixSize()))
                    .collect(Collectors.toList());
            for (RecordQueryPlanner.ScoredPlan plan : plansDescendingBySuffixSize) {
                KeyExpression planKey = orderingCompatiblePlanKey(Objects.requireNonNull(plan.planOrderingKey), candidateKey);
                if (planKey == null) {
                    return null;
                }
                candidateKey = planKey;
            }
            return candidateKey;
        }
    }

    @Nullable
    private static KeyExpression orderingCompatiblePlanKey(@Nonnull PlanOrderingKey planOrderingKey,
                                                           @Nullable KeyExpression candidateKey) {
        List<KeyExpression> components = new ArrayList<>(planOrderingKey.getSuffixSize());
        int nextNonPrefix = planOrderingKey.prefixSize;

        // Be sure to retain any elements of the candidate key
        if (candidateKey != null) {
            for (KeyExpression component : candidateKey.normalizeKeyForPositions()) {
                int pos = planOrderingKey.keys.indexOf(component);
                if (pos < 0) {
                    return null;
                }
                if (planOrderingKey.isEqualityBound(pos)) {
                    // Equality bound predicate. We're trivially ordered by it, so add it to the final key.
                    // When combining sub-plans, this can represent, slotting in all of the elements of one
                    // sub-plan before or after the elements of another sub-plan.
                    components.add(component);
                } else if (pos == nextNonPrefix) {
                    // We're aligned with the next non-prefix position. Add it in to the final key
                    components.add(component);
                    nextNonPrefix = advanceNextNonPrefix(planOrderingKey, nextNonPrefix);
                } else {
                    // Not matched. Exit now
                    return null;
                }
            }
        }

        // Add in all remaining non-prefix components. If other sub-plans are to match with this one, this
        // needs to be maintained
        while (nextNonPrefix < planOrderingKey.getKeys().size()) {
            components.add(planOrderingKey.getKeys().get(nextNonPrefix));
            nextNonPrefix = advanceNextNonPrefix(planOrderingKey, nextNonPrefix);
        }
        return combine(components);
    }

    /**
     * Is the given ordering key compatible with the candidate merge key?
     * A subkeys of the candidate key can appear in the equals part of the ordering in any order.
     * The remainder must match the ordering key after the equality.
     */
    private static boolean isOrderingCompatible(@Nonnull PlanOrderingKey planOrderingKey,
                                                @Nonnull KeyExpression candidateKey) {
        int nextNonPrefix = planOrderingKey.prefixSize;
        for (KeyExpression component : candidateKey.normalizeKeyForPositions()) {
            int pos = planOrderingKey.keys.indexOf(component);
            if (pos < 0) {
                return false;      // Not present at all.
            }
            if (planOrderingKey.isEqualityBound(pos)) {
                // A prefix equality can be in the final key anyplace, we're trivially
                // ordered by it.
                continue;
            }
            // Otherwise, components need to be in order.
            if (pos != nextNonPrefix) {
                return false;
            }
            nextNonPrefix = advanceNextNonPrefix(planOrderingKey, nextNonPrefix);
        }
        return true;
    }

    private static int advanceNextNonPrefix(@Nonnull PlanOrderingKey planOrderingKey, int nonPrefix) {
        int next = nonPrefix + 1;
        while (next < planOrderingKey.keys.size()) {
            if (!planOrderingKey.isEqualityBound(next)) {
                break;
            }
            next++;
        }
        return next;
    }

    /**
     * Find a candidate key for the sort that includes (at least) the given primary key. This finds a candidate
     * ordering key that contains the primary key columns and is compatible with as many plans as possible.
     *
     * @param plans the list of plans to check for a candidate ordering
     * @param primaryKey the primary key to preserve within the ordering key
     * @return a key that contains the primary key and is compatible with the ordering of at least one plan
     */
    @Nonnull
    public static KeyExpression candidateContainingPrimaryKey(@Nonnull Collection<RecordQueryPlanner.ScoredPlan> plans, @Nonnull KeyExpression primaryKey) {
        KeyExpression candidateKey = primaryKey;
        for (RecordQueryPlanner.ScoredPlan scoredPlan : plans) {
            PlanOrderingKey planOrderingKey = scoredPlan.planOrderingKey;
            if (!isOrderingCompatible(planOrderingKey, candidateKey)) {
                // Widen the plan to include all non-equality bound fields in the plan
                List<KeyExpression> newKeys = planOrderingKey.getKeys().subList(
                        Math.min(planOrderingKey.getPrefixSize(), planOrderingKey.getPrimaryKeyStart()),
                        planOrderingKey.getKeys().size()
                );
                candidateKey = combine(newKeys);
            }
        }
        return candidateKey;
    }

    @Nonnull
    private static KeyExpression combine(@Nonnull List<KeyExpression> keys) {
        if (keys.isEmpty()) {
            return EmptyKeyExpression.EMPTY;
        } else if (keys.stream().anyMatch(key -> key.getColumnSize() > 1)) {
            // If any of the keys have more than 1 column, then wrap the keys in a ListKeyExpression.
            // Unlike a ThenKeyExpression, this preserves the relationship that each key in keys
            // maps to exactly one column in the resulting expression. Because the key expression
            // has been normalized, this should only come up if one of the original (pre-normalization)
            // key expressions was a ListKeyExpression
            return new ListKeyExpression(keys);
        } else if (keys.size() == 1) {
            return keys.get(0);
        } else {
            return new ThenKeyExpression(keys);
        }
    }
}
