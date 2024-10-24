/*
 * PlanStringRepresentation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryExplodePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRangePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryDamPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Visitor that produces a string representation of a {@link RecordQueryPlan}. This can be used as a
 * (somewhat) compact way of explaining what is going on in the query. For example, index scans are
 * represented as:
 *
 * <pre>
 * Index(&lt;index_name&gt; &lt;scan_range&gt;)
 * </pre>
 *
 * <p>
 * Substituting in the index's name and the index scan range. Likewise, filter plans are represented by:
 * </p>
 *
 * <pre>
 * &lt;child_plan&gt; | &lt;filter&gt;
 * </pre>
 *
 * <p>
 * Here, the {@code child_plan} is calculated by recursively generating the string representation of
 * the filter plan's child plan via the same class. Note that for more complicated queries like unions or
 * intersections, there can be a lot of child plans which will be appended to the string. For very complex
 * query plans, this can result in very large query strings. For that reason, this visitor can be
 * given a {@linkplain #getMaxSize() maximum size}, after which it will stop appending new data. It
 * is recommended when logging query plans to set some maximum value to avoid logging excessively long
 * plan strings.
 * </p>
 *
 * <p>
 * Note that this class accumulates the plan string representation when it gets called. It is not safe to
 * use in multiple threads, and a new object should be created each time a new string is needed.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.AvoidStringBufferField") // Class should be short-lived
public class PlanStringRepresentation implements RecordQueryPlanVisitor<PlanStringRepresentation> {
    private final int maxSize;
    @Nonnull
    private final StringBuilder stringBuilder;
    private boolean done;

    public PlanStringRepresentation(int maxSize) {
        this.maxSize = maxSize;
        this.stringBuilder = new StringBuilder();
    }

    /**
     * Returns the maximum length of the plan string, not counting a potential trailing ellipsis.
     * If the plan string representation would exceed this amount, a trailing ellipsis is added
     * to indicate that data have been truncated. This returns {@link Integer#MAX_VALUE} if there
     * is no maximum size.
     *
     * @return the maximum size of the string representation
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Return whether the max-size of the string has already been hit. If this returns
     * {@code true}, then visiting any additional plans or appending additional data will
     * not change the returned string representation.
     *
     * @return whether the plan string representation is done appending new data
     */
    public boolean isDone() {
        return done;
    }

    /**
     * Add an arbitrary object to the string representation of a given plan. This can be useful if
     * one wants to construct a (potentially length-limited) string that combines a {@link RecordQueryPlan}
     * with other data. If this plan string representation {@linkplain #isDone() has hit its maximum size},
     * this will be a no-op and, in particular, will not call {@link Object#toString()} to construct
     * the given object's string representation. In this way, this method can be more efficient than
     * calling {@code append(toAppend.toString())}.
     *
     * @param toAppend object to append to the end of the plan string representation
     * @return this object
     * @see #append(String)
     * @see #getMaxSize()
     * @see #isDone()
     */
    @Nonnull
    public PlanStringRepresentation append(@Nullable Object toAppend) {
        if (done) {
            return this;
        }
        return append(toAppend == null ? "null" : toAppend.toString());
    }

    /**
     * Append an arbitrary string to the end of this plan representation. This can be used to
     * combine a string with plan data to form a larger (potentially length-limited) string
     * that combines a {@link RecordQueryPlan} with other data. If this plan string representation
     * {@linkplain #isDone() has hit its maximum size}, this will be a no-op.
     *
     * @param toAppend string to append to the end of the plan string representation
     * @return this object
     * @see #append(Object)
     * @see #getMaxSize()
     * @see #isDone()
     */
    @Nonnull
    public PlanStringRepresentation append(@Nonnull String toAppend) {
        if (done) {
            return this;
        }
        if (stringBuilder.length() + toAppend.length() > maxSize) {
            stringBuilder.append(toAppend, 0, maxSize - stringBuilder.length())
                    .append("...");
            done = true;
            return this;
        }
        stringBuilder.append(toAppend);
        return this;
    }

    /**
     * Append a collection of items to this plan string representation. The string representation of
     * each item will be added to the underlying string representation, with the given delimiter being
     * used to separate each one. Note that if the {@link #isDone() maximum size has been hit}, the list
     * may be truncated or may not actually be appended at all.
     * 
     * @param items the collection of items to add to the plan string representation
     * @param delimiter the delimiter to use between each item
     * @return this object
     * @see #isDone() 
     * @see #getMaxSize() 
     * @see #append(String) 
     */
    @Nonnull
    public PlanStringRepresentation appendItems(@Nonnull Collection<?> items, @Nonnull String delimiter) {
        if (done) {
            return this;
        }
        boolean first = true;
        for (Object o : items) {
            if (first) {
                first = false;
            } else {
                append(delimiter);
            }
            if (o instanceof RecordQueryPlan) {
                visit((RecordQueryPlan) o);
            } else {
                append(o);
            }
            if (done) {
                return this;
            }
        }
        return this;
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitComposedBitmapIndexQueryPlan(@Nonnull ComposedBitmapIndexQueryPlan element) {
        return visitDefault(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitAggregateIndexPlan(@Nonnull RecordQueryAggregateIndexPlan element) {
        return append("AggregateIndexScan(")
                .visit(element.getIndexPlan())
                .append(" -> ")
                .append(element.getToRecord())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitComparatorPlan(@Nonnull RecordQueryComparatorPlan element) {
        return append("COMPARATOR OF ")
                .appendItems(element.getChildren(), " ");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitCoveringIndexPlan(@Nonnull RecordQueryCoveringIndexPlan element) {
        return append("Covering(")
                .visit(element.getIndexPlan())
                .append(" -> ")
                .append(element.getToRecord())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitDeletePlan(@Nonnull RecordQueryDeletePlan element) {
        // TODO provide proper explain
        return visit(element.getChild())
                .append(" | DELETE ");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitExplodePlan(@Nonnull RecordQueryExplodePlan element) {
        return append("explode([")
                .append(element.getCollectionValue())
                .append("])");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitFetchFromPartialRecordPlan(@Nonnull RecordQueryFetchFromPartialRecordPlan element) {
        return append("Fetch(")
                .visit(element.getChild())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitFilterPlan(@Nonnull RecordQueryFilterPlan element) {
        return visit(element.getChild())
                .append(" | ")
                .append(element.getConjunctedFilter());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitFirstOrDefaultPlan(@Nonnull RecordQueryFirstOrDefaultPlan element) {
        return append("firstOrDefault(")
                .visit(element.getChild())
                .append(" || ")
                .append(element.getOnEmptyResultValue())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitFlatMapPlan(@Nonnull RecordQueryFlatMapPlan element) {
        return append("flatMap(")
                .visit(element.getOuterQuantifier().getRangesOverPlan())
                .append(", ")
                .visit(element.getInnerQuantifier().getRangesOverPlan())
                .append(")");
    }

    @Nonnull
    private PlanStringRepresentation visitInJoinPlan(@Nonnull RecordQueryInJoinPlan element) {
        final InSource inSource = element.getInSource();
        visit(element.getInnerPlan())
                .append(" WHERE ")
                .append(inSource.getBindingName())
                .append(" IN ")
                .append(inSource.valuesString());
        if (inSource.isSorted()) {
            append(" SORTED");
            if (inSource.isReverse()) {
                append(" DESC");
            }
        }
        return this;
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInComparandJoinPlan(@Nonnull RecordQueryInComparandJoinPlan element) {
        return visitInJoinPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInParameterJoinPlan(@Nonnull RecordQueryInParameterJoinPlan element) {
        return visitInJoinPlan(element);
    }

    @Nonnull
    private PlanStringRepresentation visitInUnionPlan(@Nonnull RecordQueryInUnionPlan element) {
        return append("∪(")
                .appendItems(element.getInSources(), ", ")
                .append(") ")
                .visit(element.getChild());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInUnionOnKeyExpressionPlan(@Nonnull RecordQueryInUnionOnKeyExpressionPlan element) {
        return visitInUnionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInUnionOnValuesPlan(@Nonnull RecordQueryInUnionOnValuesPlan element) {
        return visitInUnionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInValuesJoinPlan(@Nonnull RecordQueryInValuesJoinPlan element) {
        return visitInJoinPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitIndexPlan(@Nonnull RecordQueryIndexPlan element) {
        final IndexScanParameters scanParameters = element.getScanParameters();
        append("Index(")
                .append(element.getIndexName())
                .append(" ")
                .append(element.getScanParameters().getScanDetails());
        if (!IndexScanType.BY_VALUE.equals(scanParameters.getScanType())) {
            append(" ").append(scanParameters.getScanType());
        }
        if (element.isReverse()) {
            append(" REVERSE");
        }
        return append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitInsertPlan(@Nonnull RecordQueryInsertPlan element) {
        return visit(element.getChild())
                .append(" | INSERT INTO ")
                .append(element.getTargetRecordType());
    }

    @Nonnull
    private PlanStringRepresentation visitIntersectionPlan(@Nonnull RecordQueryIntersectionPlan element) {
        return appendItems(element.getChildren(), " ∩ ");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitIntersectionOnKeyExpressionPlan(@Nonnull RecordQueryIntersectionOnKeyExpressionPlan element) {
        return visitIntersectionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitIntersectionOnValuesPlan(@Nonnull RecordQueryIntersectionOnValuesPlan element) {
        return visitIntersectionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitLoadByKeysPlan(@Nonnull RecordQueryLoadByKeysPlan element) {
        return append("ByKeys(")
                .append(element.getKeysSource())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitMapPlan(@Nonnull RecordQueryMapPlan element) {
        return append("map(")
                .visit(element.getChild())
                .append("[")
                .append(element.getResultValue())
                .append("])");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitPredicatesFilterPlan(@Nonnull RecordQueryPredicatesFilterPlan element) {
        return visit(element.getChild())
                .append(" | ")
                .append(element.getConjunctedPredicate());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitRangePlan(@Nonnull RecordQueryRangePlan element) {
        return append("Range(")
                .append(element.getExclusiveLimitValue())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitScanPlan(@Nonnull RecordQueryScanPlan element) {
        final TupleRange tupleRange = element.getScanComparisons().toTupleRangeWithoutContext();
        return append("Scan(")
                .append(tupleRange == null ? element.getScanComparisons() : tupleRange)
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitScoreForRankPlan(@Nonnull RecordQueryScoreForRankPlan element) {
        return visit(element.getChild())
                .append(" WHERE ")
                .appendItems(element.getRanks(), ", ");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitSelectorPlan(@Nonnull RecordQuerySelectorPlan element) {
        return append("SELECTOR OF ")
                .appendItems(element.getChildren(), " ");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitStreamingAggregationPlan(@Nonnull RecordQueryStreamingAggregationPlan element) {
        return visit(element.getChild())
                .append(" | AGGREGATE BY ")
                .append(element.getAggregateValue())
                .append(", GROUP BY ")
                .append(element.getGroupingValue());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitTextIndexPlan(@Nonnull RecordQueryTextIndexPlan element) {
        final TextScan textScan = element.getTextScan();
        return append("TextIndex(")
                .append(textScan.getIndex().getName())
                .append(" ")
                .append(textScan.getGroupingComparisons())
                .append(", ")
                .append(textScan.getTextComparison())
                .append(", ")
                .append(textScan.getSuffixComparisons())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitTypeFilterPlan(@Nonnull RecordQueryTypeFilterPlan element) {
        return visit(element.getChild())
                .append(" | ")
                .append(element.getRecordTypes());
    }

    @Nonnull
    private PlanStringRepresentation visitUnionPlan(@Nonnull RecordQueryUnionPlanBase element) {
        return appendItems(element.getChildren(), element.getDelimiter());
    }

    @Nonnull PlanStringRepresentation visitRecursiveUnionPlan(@Nonnull RecordQueryRecursiveUnorderedUnionPlan element) {
        return appendItems(element.getChildren(), " ↻∪ "); // U+21BB U+222A
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUnionOnKeyExpressionPlan(@Nonnull RecordQueryUnionOnKeyExpressionPlan element) {
        return visitUnionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUnionOnValuesPlan(@Nonnull RecordQueryUnionOnValuesPlan element) {
        return visitUnionPlan(element);
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUnorderedDistinctPlan(@Nonnull RecordQueryUnorderedDistinctPlan element) {
        return visit(element.getChild())
                .append(" | UnorderedDistinct(")
                .append(element.getComparisonKey())
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUnorderedPrimaryKeyDistinctPlan(@Nonnull RecordQueryUnorderedPrimaryKeyDistinctPlan element) {
        return visit(element.getChild())
                .append(" | UnorderedPrimaryKeyDistinct()");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUnorderedUnionPlan(@Nonnull RecordQueryUnorderedUnionPlan element) {
        return append("Unordered(")
                .visitUnionPlan(element)
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitRecursiveUnorderedUnionPlan(@Nonnull final RecordQueryRecursiveUnorderedUnionPlan recursiveUnorderedUnionPlan) {
        return append("RecursiveUnordered(")
                .visitRecursiveUnorderedUnionPlan(recursiveUnorderedUnionPlan)
                .append(")");
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitUpdatePlan(@Nonnull RecordQueryUpdatePlan element) {
        // TODO provide proper explain
        return visit(element.getInnerPlan())
                .append(" | UPDATE ")
                .append(element.getTargetRecordType());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitDamPlan(@Nonnull RecordQueryDamPlan element) {
        return visit(element.getChild())
                .append(" | DAM ")
                .append(element.getKey());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitSortPlan(@Nonnull RecordQuerySortPlan element) {
        return visit(element.getChild())
                .append(" ORDER BY ")
                .append(element.getKey());
    }

    @Nonnull
    @Override
    public PlanStringRepresentation visitDefault(@Nonnull RecordQueryPlan element) {
        return append(element.toString());
    }

    @Nonnull
    @Override
    public String toString() {
        return stringBuilder.toString();
    }

    @Nonnull
    public static String toString(@Nonnull RecordQueryPlan plan, int maxSize) {
        PlanStringRepresentation visitor = new PlanStringRepresentation(maxSize);
        return visitor.visit(plan).toString();
    }

    @Nonnull
    public static String toString(@Nonnull RecordQueryPlan plan) {
        return toString(plan, Integer.MAX_VALUE);
    }
}
