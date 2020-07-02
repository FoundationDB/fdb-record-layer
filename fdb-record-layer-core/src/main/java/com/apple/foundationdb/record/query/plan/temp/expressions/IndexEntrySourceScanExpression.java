/*
 * IndexEntrySourceScanExpression.java
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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.rules.LogicalToPhysicalScanRule;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical version of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} that represents
 * a partially-implemented index scan. The primary difference between the two is that a {@code RecordQueryIndexPlan}
 * requires a fully formed {@link com.apple.foundationdb.record.query.plan.ScanComparisons}, which does not track
 * which comparisons belong to which parts of the index's key expression. In contrast, this logical index scan has a
 * {@link ViewExpressionComparisons} which explicitly tracks that information. Except for the final "implementation"
 * rules, a planner rule should generally prefer to produce and consume {@code IndexEntrySourceScanExpression}s so that
 * important information about the index key expression is retained.
 *
 * @see LogicalToPhysicalScanRule which converts this to a {@code RecordQueryIndexPlan}
 */
public class IndexEntrySourceScanExpression implements RelationalExpression {
    @Nonnull
    private final IndexEntrySource indexEntrySource;
    @Nonnull
    private final IndexScanType scanType;
    @Nonnull
    private final ViewExpressionComparisons comparisons;
    private final boolean reverse;

    public IndexEntrySourceScanExpression(@Nonnull final IndexEntrySource indexEntrySource, @Nonnull IndexScanType scanType,
                                          @Nonnull final ViewExpressionComparisons comparisons, final boolean reverse) {
        this.indexEntrySource = indexEntrySource;
        this.scanType = scanType;
        this.comparisons = comparisons;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    public IndexEntrySource getIndexEntrySource() {
        return indexEntrySource;
    }

    @Nullable
    public String getIndexName() {
        return indexEntrySource.getIndexName();
    }

    @Nonnull
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public ViewExpressionComparisons getComparisons() {
        return comparisons;
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        // TODO this may actually be correlated
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public IndexEntrySourceScanExpression rebase(@Nonnull final AliasMap translationMap) {
        // TODO this may actually be correlated in this expression and we must rebase() properly here
        return this;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final IndexEntrySourceScanExpression other = (IndexEntrySourceScanExpression) otherExpression;
        return indexEntrySource.equals(other.indexEntrySource) &&
               scanType.equals(other.scanType) &&
               comparisons.equals(other.comparisons) &&
               reverse == other.reverse;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return resultEquals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexEntrySource, scanType, comparisons, reverse);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("IndexSourceScan(");
        str.append(indexEntrySource).append(" ");
        str.append(comparisons.toScanComparisons()).append(" ");
        if (scanType != IndexScanType.BY_VALUE) {
            str.append(scanType);
        }
        if (reverse) {
            str.append(" REVERSE");
        }
        return str.toString();
    }
}
