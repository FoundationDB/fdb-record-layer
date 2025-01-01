/*
 * ExplainPlanVisitor.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExplainFormatterWithColor;
import com.apple.foundationdb.record.query.plan.cascades.ExplainSelfContainedSymbolMap;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAggregateIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryDamPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

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
public class ExplainPlanVisitor extends ExplainTokens implements RecordQueryPlanVisitor<ExplainTokens> {
    private final int maxSize;

    private boolean done;

    public ExplainPlanVisitor(final int maxSize) {
        this.maxSize = maxSize;
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

    @Override
    @Nonnull
    public ExplainTokens add(@Nonnull final ExplainTokens.Token toAppend) {
        if (done) {
            return this;
        }

        if (getMinLength() + toAppend.getMinLength() > maxSize) {
            super.add(toAppend).addToString("...");
            done = true;
            return this;
        }
        super.add(toAppend);
        return this;
    }

    private ExplainPlanVisitor visitAndJoin(@Nonnull final Supplier<ExplainTokens> delimiterExplainTokensSupplier,
                                            @Nonnull final Iterable<? extends RecordQueryPlan> plans) {
        for (final var iterator = plans.iterator(); iterator.hasNext(); ) {
            final var plan = iterator.next();
            visit(plan);
            if (iterator.hasNext()) {
                addAll(delimiterExplainTokensSupplier.get().getTokens());
            }
        }
        return this;
    }

    @Nonnull
    @Override
    public ExplainTokens visitComposedBitmapIndexQueryPlan(@Nonnull final ComposedBitmapIndexQueryPlan element) {
        return addToString(element.toString());
    }

    @Nonnull
    @Override
    public ExplainTokens visitAggregateIndexPlan(@Nonnull final RecordQueryAggregateIndexPlan element) {
        addIdentifier("AISCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        visit(element.getIndexPlan());
        return addWhitespace().addToString("->").addWhitespace()
                .addToString(element.getToRecord())
                .addOptionalWhitespace()
                .addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
        addIdentifier("COMPARATOR").addWhitespace().addIdentifier("OF").addWhitespace();
        return visitAndJoin(() -> new ExplainTokens().addCommaAndWhiteSpace(), comparatorPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
        addIdentifier("COVERING").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        visit(coveringIndexPlan.getIndexPlan());
        return addWhitespace().addToString("->").addWhitespace()
                .addToString(coveringIndexPlan.getToRecord())
                .addOptionalWhitespace()
                .addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
        // TODO provide proper explain
        visit(deletePlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("DELETE");
    }

    @Nonnull
    @Override
    public ExplainTokens visitExplodePlan(@Nonnull final RecordQueryExplodePlan explodePlan) {
        return addFunctionCall("EXPLODE",
                Value.explainFunctionArguments(ImmutableList.of(() -> explodePlan.getCollectionValue().explain())));
    }

    @Nonnull
    @Override
    public ExplainTokens visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan fromPartialRecordPlan) {
        visit(fromPartialRecordPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("FETCH");
    }

    @Nonnull
    @Override
    public ExplainTokens visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
        visit(filterPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("QCFILTER").addWhitespace()
                .addAliasDefinition(filterPlan.getInner().getAlias())
                .addToString(filterPlan.getConjunctedFilter());
    }

    @Nonnull
    @Override
    public ExplainTokens visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan) {
        visit(firstOrDefaultPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace()
                .addIdentifier("DEFAULT").addWhitespace()
                .addNested(firstOrDefaultPlan.getOnEmptyResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitDefaultOnEmptyPlan(@Nonnull final RecordQueryDefaultOnEmptyPlan defaultOnEmptyPlan) {
        visit(defaultOnEmptyPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace()
                .addIdentifier("ON").addWhitespace().addIdentifier("EMPTY").addWhitespace()
                .addNested(defaultOnEmptyPlan.getOnEmptyResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
        final var outerQuantifier = flatMapPlan.getOuterQuantifier();
        visit(outerQuantifier.getRangesOverPlan());
        addWhitespace().addToString("|").addWhitespace().addIdentifier("FLATMAP").addWhitespace()
                .addAliasDefinition(outerQuantifier.getAlias()).addToString("->").addWhitespace()
                .addOpeningBrace().addWhitespace();
        return visit(flatMapPlan.getInnerQuantifier().getRangesOverPlan()).addWhitespace().addClosingBrace();
    }

    @Nonnull
    private ExplainTokens visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
        final var inSource = inJoinPlan.getInSource();
        final var isCorrelation = Bindings.Internal.CORRELATION.isOfType(inSource.getBindingName());
        final var bindingName =
                isCorrelation
                ? Bindings.Internal.CORRELATION.identifier(inSource.getBindingName())
                : inSource.getBindingName();

        addOpeningBracket().addOptionalWhitespace().addNested(inSource.explain().getExplainTokens())
                .addOptionalWhitespace().addClosingBracket().addWhitespace()
                .addToString("|").addWhitespace().addIdentifier("INJOIN").addWhitespace()
                .addAliasDefinition(CorrelationIdentifier.of(bindingName)).addWhitespace().addToString("->")
                .addWhitespace().addOpeningBrace().addWhitespace();
        return visit(inJoinPlan.getChild()).addWhitespace().addClosingBrace();
    }

    @Nonnull
    @Override
    public ExplainTokens visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
        return visitInJoinPlan(inComparandJoinPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
        return visitInJoinPlan(inParameterJoinPlan);
    }

    @Nonnull
    private ExplainTokens visitInUnionPlan(@Nonnull final RecordQueryInUnionPlan inUnionPlan) {
        final var resultExplainTokens = new ExplainTokens();
        final var inSourcesBuilder = ImmutableList.<ExplainTokens>builder();
        final var bindingsBuilder = ImmutableList.<ExplainTokens>builder();

        for (final var inSource : inUnionPlan.getInSources()) {
            inSourcesBuilder.add(inSource.explain().getExplainTokens());

            final var isCorrelation = Bindings.Internal.CORRELATION.isOfType(inSource.getBindingName());
            final var bindingName =
                    isCorrelation
                    ? Bindings.Internal.CORRELATION.identifier(inSource.getBindingName())
                    : inSource.getBindingName();
            bindingsBuilder.add(new ExplainTokens().addAliasDefinition(CorrelationIdentifier.of(bindingName)));
        }

        resultExplainTokens.addOpeningParen().addOptionalWhitespace()
                .addSequence(() -> new ExplainTokens().addWhitespace().addToString("⋈").addWhitespace(),
                        inSourcesBuilder.build())
                .addOptionalWhitespace().addClosingParen()
                .addWhitespace().addToString("⋓").addWhitespace()
                .addOpeningParen().addOptionalWhitespace()
                .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), bindingsBuilder.build())
                .addWhitespace().addToString("->").addWhitespace()
                .addOpeningBrace().addWhitespace();
        return visit(inUnionPlan.getChild()).addWhitespace().addClosingBrace();
    }

    @Nonnull
    @Override
    public ExplainTokens visitInUnionOnKeyExpressionPlan(@Nonnull final RecordQueryInUnionOnKeyExpressionPlan inUnionOnKeyExpressionPlan) {
        return visitInUnionPlan(inUnionOnKeyExpressionPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitInUnionOnValuesPlan(@Nonnull final RecordQueryInUnionOnValuesPlan inUnionOnValuesPlan) {
        return visitInUnionPlan(inUnionOnValuesPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitInValuesJoinPlan(@Nonnull final RecordQueryInValuesJoinPlan inValuesJoinPlan) {
        return visitInJoinPlan(inValuesJoinPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitIndexPlan(@Nonnull final RecordQueryIndexPlan indexPlan) {
        final IndexScanParameters scanParameters = indexPlan.getScanParameters();
        addIdentifier("ISCAN").addOptionalWhitespace().addOpeningParen().addToString(indexPlan.getIndexName()).addWhitespace()
                .addNested(indexPlan.getScanParameters().explain().getExplainTokens());
        if (!IndexScanType.BY_VALUE.equals(scanParameters.getScanType())) {
            addWhitespace().addIdentifier(scanParameters.getScanType().toString());
        }
        if (indexPlan.isReverse()) {
            addWhitespace().addIdentifier("REVERSE");
        }
        return addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
        // TODO provide proper explain
        visit(insertPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("INSERT")
                .addWhitespace().addIdentifier("INTO").addWhitespace().addToString(insertPlan.getTargetRecordType());
    }

    @Nonnull
    @Override
    public ExplainTokens visitTempTableInsertPlan(@Nonnull final TempTableInsertPlan tempTableInsertPlan) {
        // TODO provide proper explain
        visit(tempTableInsertPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("INSERT").addWhitespace()
                .addIdentifier("INTO").addWhitespace().addIdentifier("TEMP").addWhitespace()
                .addNested(tempTableInsertPlan.getTempTableReferenceValue().explain().getExplainTokens());
    }

    @Nonnull
    private ExplainTokens visitIntersectionPlan(@Nonnull final RecordQueryIntersectionPlan intersectionPlan) {
        visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∩").addWhitespace(),
                intersectionPlan.getChildren());
        return addWhitespace().addIdentifier("COMPARE").addWhitespace().addIdentifier("BY").addWhitespace()
                .addNested(intersectionPlan.getComparisonKeyFunction().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionOnKeyExpressionPlan) {
        return visitIntersectionPlan(intersectionOnKeyExpressionPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
        return visitIntersectionPlan(intersectionOnValuesPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitLoadByKeysPlan(@Nonnull final RecordQueryLoadByKeysPlan loadByKeysPlan) {
        return addIdentifier("BYKEYS").addWhitespace().addOpeningParen().addOptionalWhitespace()
                .addToString(loadByKeysPlan.getKeysSource())
                .addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
        visit(mapPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("MAP").addWhitespace()
                .addAliasDefinition(mapPlan.getInner().getAlias())
                .addNested(mapPlan.getResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        visit(predicatesFilterPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("FILTER").addWhitespace()
                .addAliasDefinition(predicatesFilterPlan.getInner().getAlias())
                .addNested(predicatesFilterPlan.getConjunctedPredicate().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitRangePlan(@Nonnull RecordQueryRangePlan element) {
        return addIdentifier("RANGE").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                .addToString(element.getExclusiveLimitValue())
                .addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
        final var scanComparisons = scanPlan.getScanComparisons();
        final var tupleRange = scanComparisons.toTupleRangeWithoutContext();
        addIdentifier("SCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        if (tupleRange == null) {
            addNested(scanComparisons.explain().getExplainTokens());
        } else {
            addToString(tupleRange);
        }
        return addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitScoreForRankPlan(@Nonnull final RecordQueryScoreForRankPlan scoreForRankPlan) {
        visit(scoreForRankPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("SRANK").addWhitespace()
                .addToStrings(scoreForRankPlan.getRanks());
    }

    @Nonnull
    @Override
    public ExplainPlanVisitor visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
        addIdentifier("SELECTOR").addWhitespace().addIdentifier("OF").addWhitespace();
        return visitAndJoin(() -> new ExplainTokens().addCommaAndWhiteSpace(), selectorPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan streamingAggregationPlan) {
        visit(streamingAggregationPlan.getChild());
        addWhitespace().addToString("|").addWhitespace().addIdentifier("AGG").addWhitespace()
                .addAliasDefinition(streamingAggregationPlan.getInner().getAlias())
                .addNested(streamingAggregationPlan.getAggregateValue().explain().getExplainTokens());
        final var groupingValue = streamingAggregationPlan.getGroupingValue();
        if (groupingValue != null) {
            return addWhitespace().addIdentifier("GROUP").addWhitespace().addIdentifier("BY").addWhitespace()
                    .addNested(groupingValue.explain().getExplainTokens());
        }
        return this;
    }

    @Nonnull
    @Override
    public ExplainTokens visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan textIndexPlan) {
        final TextScan textScan = textIndexPlan.getTextScan();
        addIdentifier("TISCAN").addOptionalWhitespace().addOptionalWhitespace().addOptionalWhitespace()
                .addToString(textScan.getIndex().getName()).addCommaAndWhiteSpace();
        if (textScan.getGroupingComparisons() != null) {
            addNested(textScan.getGroupingComparisons().explain().getExplainTokens());
        } else {
            addToString("NULL");
        }
        addCommaAndWhiteSpace()
                .addNested(textScan.getTextComparison().explain().getExplainTokens()).addCommaAndWhiteSpace();
        if (textScan.getSuffixComparisons() != null) {
            addNested(textScan.getSuffixComparisons().explain().getExplainTokens());
        } else {
            addToString("NULL");
        }
        return addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
        visit(typeFilterPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("TFILTER").addWhitespace()
                .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        () -> typeFilterPlan.getRecordTypes()
                                .stream()
                                .map(recordType -> new ExplainTokens().addToString(recordType))
                                .iterator());
    }

    @Nonnull
    private ExplainTokens visitUnionPlan(@Nonnull final RecordQueryUnionPlan unionPlan) {
        visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∪").addWhitespace(),
                unionPlan.getChildren());

        return addWhitespace().addIdentifier("COMPARE").addWhitespace().addIdentifier("BY").addWhitespace()
                .addNested(unionPlan.getComparisonKeyFunction().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnionOnKeyExpressionPlan(@Nonnull final RecordQueryUnionOnKeyExpressionPlan unionOnKeyExpressionPlan) {
        return visitUnionPlan(unionOnKeyExpressionPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionOnValuesPlan) {
        return visitUnionPlan(unionOnValuesPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
        visit(unorderedDistinctPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace()
                .addIdentifier("DISTINCT").addWhitespace().addIdentifier("BY").addWhitespace()
                .addToString(unorderedDistinctPlan.getComparisonKey());
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
        visit(unorderedPrimaryKeyDistinctPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace()
                .addIdentifier("DISTINCT").addWhitespace().addIdentifier("BY").addWhitespace().addIdentifier("PK");
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
        return visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∪").addWhitespace(),
                unorderedUnionPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
        // TODO provide proper explain
        visit(updatePlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("UPDATE").addWhitespace()
                .addToString(updatePlan.getTargetRecordType());
    }

    @Nonnull
    @Override
    public ExplainTokens visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
        visit(damPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace().addIdentifier("DAM");
    }

    @Nonnull
    @Override
    public ExplainTokens visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
        visit(sortPlan.getChild());
        return addWhitespace().addToString("|").addWhitespace()
                .addIdentifier("SORT").addWhitespace().addIdentifier("BY").addWhitespace()
                .addToString(sortPlan.getKey());
    }

    @Nonnull
    @Override
    public ExplainTokens visitTempTableScanPlan(@Nonnull final TempTableScanPlan tempTableScanPlan) {
        return addIdentifier("TEMP").addWhitespace().addIdentifier("SCAN").addWhitespace()
                .addNested(tempTableScanPlan.getResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visit(@Nonnull final RecordQueryPlan element) {
        if (done) {
            return this;
        }
        return RecordQueryPlanVisitor.super.visit(element);
    }

    @Nonnull
    @Override
    public ExplainPlanVisitor visitDefault(@Nonnull RecordQueryPlan element) {
        throw new RecordCoreException("no default implementation");
    }

    @Nonnull
    public static String toString(@Nonnull RecordQueryPlan plan, int maxSize) {
        ExplainPlanVisitor visitor = new ExplainPlanVisitor(maxSize);
        return visitor.visit(plan).render(new ExplainFormatterWithColor(ExplainSelfContainedSymbolMap::new));
    }

    @Nonnull
    public static String toString(@Nonnull RecordQueryPlan plan) {
        return toString(plan, Integer.MAX_VALUE);
    }
}
