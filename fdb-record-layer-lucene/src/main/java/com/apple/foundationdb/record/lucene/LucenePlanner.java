/*
 * LucenePlanner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.LuceneQueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A planner to implement lucene query planning so that we can isolate the lucene functionality to
 * a distinct package. This was necessary because of the need to pass the sort key expression into the
 * plan created and from there into the record cursor which creates a Lucene sort object.
 */
public class LucenePlanner extends RecordQueryPlanner {

    public LucenePlanner(@Nonnull final RecordMetaData metaData, @Nonnull final RecordStoreState recordStoreState, final PlannableIndexTypes indexTypes, final FDBStoreTimer timer) {
        super(metaData, recordStoreState);
    }

    private ScanComparisons getScanForAndLucene(@Nonnull AndComponent filter, @Nullable FilterSatisfiedMask filterMask) {
        final Iterator<FilterSatisfiedMask> subFilterMasks = filterMask != null ? filterMask.getChildren().iterator() : null;
        final List<QueryComponent> filters = filter.getChildren();
        ScanComparisons scanComparisons = ScanComparisons.EMPTY;
        for (QueryComponent subFilter : filters) {
            final FilterSatisfiedMask childMask = subFilterMasks != null ? subFilterMasks.next() : null;
            ScanComparisons children = getComparisonsForLuceneFilter(subFilter, childMask);
            if (children != null) {
                childMask.setSatisfied(true);
                return children;
            }
        }
        return scanComparisons;
    }

    private ScanComparisons getComparisonsForLuceneFilter(@Nonnull QueryComponent filter, FilterSatisfiedMask filterMask) {
        if (filter instanceof AndComponent) {
            return getScanForAndLucene((AndComponent) filter, filterMask);
        } else if (filter instanceof LuceneQueryComponent) {
            filterMask.setSatisfied(true);
            return ScanComparisons.from(((LuceneQueryComponent)filter).getComparison());
        }
        return null;
    }


    @Override
    protected ScoredPlan planLucene(@Nonnull CandidateScan candidateScan,
                                    @Nonnull Index index, @Nonnull QueryComponent filter,
                                    @Nullable KeyExpression sort) {
        FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);
        final ScanComparisons scans = getComparisonsForLuceneFilter(filter, filterMask);
        if (scans == null) {
            return null;
        }
        RecordQueryPlan plan;
        plan = new RecordQueryIndexPlan(index.getName(), IndexScanType.BY_LUCENE, scans, false);
        plan = addTypeFilterIfNeeded(candidateScan, plan, getPossibleTypes(index));
        return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(), 100,
                false, null);
    }
}
