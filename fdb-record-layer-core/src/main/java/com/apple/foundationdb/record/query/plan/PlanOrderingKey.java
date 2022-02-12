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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

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

    public PlanOrderingKey(@Nonnull List<KeyExpression> keys, int prefixSize,
                           int primaryKeyStart, int primaryKeyTail) {
        this.keys = keys;
        this.prefixSize = prefixSize;
        this.primaryKeyStart = primaryKeyStart;
        this.primaryKeyTail = primaryKeyTail;
    }

    @Nonnull
    public List<KeyExpression> getKeys() {
        return keys;
    }

    public int getPrefixSize() {
        return prefixSize;
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

    @Nullable
    public static PlanOrderingKey forPlan(@Nonnull RecordMetaData metaData, @Nonnull RecordQueryPlan queryPlan,
                                          @Nullable KeyExpression primaryKey) {
        if (primaryKey == null) {
            return null;
        }
        while (queryPlan instanceof RecordQueryFilterPlan) {
            queryPlan = ((RecordQueryFilterPlan)queryPlan).getInnerPlan();
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
                if (!((RecordQueryIndexPlan)indexPlan).hasComparisons()) {
                    return null;
                }
                prefixSize = ((RecordQueryIndexPlan)indexPlan).getComparisons().getEqualitySize();
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
                                       scanPlan.getComparisons().getEqualitySize(),
                                       0, 0);
        } else if (queryPlan instanceof RecordQueryIntersectionPlan) {
            return forComparisonKey(((RecordQueryIntersectionPlan)queryPlan).getComparisonKey(), primaryKey);
        } else if (queryPlan instanceof RecordQueryUnionPlan) {
            return forComparisonKey(((RecordQueryUnionPlan)queryPlan).getComparisonKey(), primaryKey);
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
     * @param plans the plans for which to find a compatible ordering
     * @param candidateKey a suggested key to use
     * @param candidateOnly only accept the given candidate
     * @return a comparison key compatible with all the plans
     */
    @Nullable
    public static KeyExpression mergedComparisonKey(@Nonnull List<RecordQueryPlanner.ScoredPlan> plans,
                                                    @Nullable KeyExpression candidateKey,
                                                    boolean candidateOnly) {
        if (candidateOnly) {
            if (candidateKey == null) {
                return null;
            }
        } else if (candidateKey == null) {
            candidateKey = sufficientOrdering(plans.get(0).planOrderingKey);
        } else {
            // Allowed to use other than just given key; check it first and if not pick one based on one of the plans.
            try_candidate_key:
            {
                for (RecordQueryPlanner.ScoredPlan plan : plans) {
                    final PlanOrderingKey planOrderingKey = plan.planOrderingKey;
                    if (!isOrderingCompatible(planOrderingKey, candidateKey)) {
                        candidateKey = sufficientOrdering(planOrderingKey);
                        break try_candidate_key;
                    }
                }
                return candidateKey;
            }
        }
        // See whether candidateKey, the only one allowed or one sufficient for one of the plans, is good enough for all plans.
        for (RecordQueryPlanner.ScoredPlan plan : plans) {
            final PlanOrderingKey planOrderingKey = plan.planOrderingKey;
            if (!isOrderingCompatible(planOrderingKey, candidateKey)) {
                return null;
            }
        }
        return candidateKey;
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
            if (pos < planOrderingKey.prefixSize) {
                // A prefix equality can be in the final key anyplace, we're trivially
                // ordered by it.
                continue;
            }
            // Otherwise, components need to be in order.
            if (pos != nextNonPrefix) {
                return false;
            }
            nextNonPrefix++;
        }
        return true;
    }

    /**
     * Get the smallest key that is sufficient to generate the primary key while still obeying the ordering key.
     * This will include all of the primary key, with perhaps some non-equality keys before it.
     */
    @Nonnull
    private static KeyExpression sufficientOrdering(@Nonnull PlanOrderingKey planOrderingKey) {
        List<KeyExpression> keys = planOrderingKey.keys
                .subList(Math.min(planOrderingKey.prefixSize, planOrderingKey.primaryKeyStart),
                        planOrderingKey.keys.size());
        if (keys.size() == 1) {
            return keys.get(0);
        } else {
            return new ThenKeyExpression(keys);
        }
    }

}
