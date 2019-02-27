/*
 * LogicalIndexScanExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;

/**
 * A logical version of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} that represents
 * a partially-implemented index scan. The primary difference between the two is that a {@code RecordQueryIndexPlan}
 * requires a fully formed {@link com.apple.foundationdb.record.query.plan.ScanComparisons}, which does not track
 * which comparisons belong to which parts of the index's key expression. In contrast, this logical index scan has a
 * {@link KeyExpressionComparisons} which explicitly tracks that information. Except for the final "implementation"
 * rules, a planner rule should generally prefer to produce and consume {@code LogicalIndexScanExpression}s so that
 * important information about the index key expression is retained.
 *
 * @see com.apple.foundationdb.record.query.plan.temp.rules.LogicalToPhysicalIndexScanRule which converts this to a {@code RecordQueryIndexPlan}
 */
public class LogicalIndexScanExpression implements RelationalPlannerExpression {
    @Nonnull
    private final String indexName;
    @Nonnull
    private final IndexScanType scanType;
    @Nonnull
    private final KeyExpressionComparisons comparisons;
    private final boolean reverse;

    public LogicalIndexScanExpression(@Nonnull final String indexName, @Nonnull IndexScanType scanType,
                                      @Nonnull final KeyExpressionComparisons comparisons, final boolean reverse) {
        this.indexName = indexName;
        this.scanType = scanType;
        this.comparisons = comparisons;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Nonnull
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public KeyExpressionComparisons getComparisons() {
        return comparisons;
    }

    public boolean isReverse() {
        return reverse;
    }
}
