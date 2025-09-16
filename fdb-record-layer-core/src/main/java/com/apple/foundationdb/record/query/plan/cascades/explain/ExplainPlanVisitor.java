/*
 * ExplainPlanVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.explain;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainLevel;
import com.apple.foundationdb.record.query.plan.explain.ExplainSelfContainedSymbolMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.PrettyExplainFormatter;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMultiIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithExplain;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRangePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursivePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTableFunctionPlan;
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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Visitor that produces a string representation of a {@link RecordQueryPlan}. This can be used as a
 * (somewhat) compact way of explaining what is going on in the query. For example, index scans are
 * represented as:
 *
 * <pre>
 * ISCAN(&lt;index_name&gt; &lt;scan_range&gt;)
 * </pre>
 *
 * <p>
 * Substituting in the index's name and the index scan range. Likewise, filter plans are represented by:
 * </p>
 *
 * <pre>
 * &lt;child_plan&gt; | FILTER &lt;filter&gt;
 * </pre>
 *
 * <p>
 * We generate the explain string of a query plan in two phases. In the first phase, a token list is generated
 * that is then rendered in the second phase.
 * </p>
 * <p>
 * The logic to generate the token list is recursive and arranged using
 * the visitor pattern. While this visitor can be considered the central hub of all individual plan-specific code,
 * plenty of other explain-related logic resides (and should continue to do so) in several
 * {@link com.apple.foundationdb.record.query.plan.cascades.TreeLike} hierarchies such as
 * {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} and
 * {@link com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate}.
 * </p>
 * <p>
 * The token list is then rendered using a {@link ExplainFormatter}
 * which in turn employs a {@link ExplainSymbolMap} or derivatives
 * to actually render the tokens into a char sequence. While the formatter may inject whitespaces, line breaks, or
 * ANSI escape sequences (for color and in general prettification), the symbol map is used to resolve aliases used
 * by the planner into a string. That resolved string may just be the alias itself (for debugging purposes) or some
 * other alias that has desirable properties like stability across JVMs.
 * </p>
 * <p>
 * By convention all plans can be stringified which is replaced by a direct call to this visitor:
 * <pre>
 * {@code
 * ExplainVisitor.toStringForDebugging(this);
 * }
 * </pre>
 * which preserves aliases and has no maximum string length boundary.
 * </p>
 *
 * <p>
 * Note that objects of this class are stateful. It is not safe to use the same visitor in multiple threads
 * and a new object should be created each time a new explain is needed.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ExplainPlanVisitor extends ExplainTokens implements RecordQueryPlanVisitor<ExplainTokens> {
    private final int maxSize;
    private boolean done;

    public ExplainPlanVisitor(final int maxSize) {
        this.maxSize = maxSize;
        this.done = false;
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

        super.add(toAppend);
        if (Arrays.stream(getMinLengths()).allMatch(minLength -> minLength > maxSize)) {
            done = true;
        }
        return this;
    }

    @Nonnull
    private ExplainTokens pipe() {
        addLinebreakOrWhitespace().addToString("|").addWhitespace();
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
        addKeyword("AISCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        addNested(indexDetails(element.getIndexPlan()));
        return addWhitespace().addToString("->").addWhitespace()
                .addToString(element.getToRecord())
                .addOptionalWhitespace()
                .addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitComparatorPlan(@Nonnull final RecordQueryComparatorPlan comparatorPlan) {
        addKeyword("COMPARATOR").addWhitespace().addKeyword("OF").addWhitespace();
        return visitAndJoin(() -> new ExplainTokens().addCommaAndWhiteSpace(), comparatorPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan coveringIndexPlan) {
        addKeyword("COVERING").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        final var underlyingWithIndex = coveringIndexPlan.getIndexPlan();
        if (underlyingWithIndex instanceof RecordQueryIndexPlan) {
            addNested(indexDetails((RecordQueryIndexPlan)underlyingWithIndex));
        } else if (underlyingWithIndex instanceof RecordQueryTextIndexPlan) {
            textIndexDetails((RecordQueryTextIndexPlan)underlyingWithIndex);
        } else {
            addIdentifier(underlyingWithIndex.getIndexName());
        }
        return addNested(ExplainLevel.ALL_DETAILS, new ExplainTokens().addWhitespace().addToString("->")
                .addWhitespace().addToString(coveringIndexPlan.getToRecord()))
                .addOptionalWhitespace()
                .addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitDeletePlan(@Nonnull final RecordQueryDeletePlan deletePlan) {
        // TODO provide proper explain
        visit(deletePlan.getChild());
        return pipe().addKeyword("DELETE");
    }

    @Nonnull
    @Override
    public ExplainTokens visitExplodePlan(@Nonnull final RecordQueryExplodePlan explodePlan) {
        return addKeyword("EXPLODE").addWhitespace()
                .addNested(explodePlan.getCollectionValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitFetchFromPartialRecordPlan(@Nonnull final RecordQueryFetchFromPartialRecordPlan fromPartialRecordPlan) {
        visit(fromPartialRecordPlan.getChild());
        return pipe().addKeyword("FETCH");
    }

    @Nonnull
    @Override
    public ExplainTokens visitFilterPlan(@Nonnull final RecordQueryFilterPlan filterPlan) {
        visit(filterPlan.getChild());
        return pipe().addKeyword("QCFILTER").addWhitespace()
                .addToString(filterPlan.getConjunctedFilter());
    }

    @Nonnull
    @Override
    public ExplainTokens visitFirstOrDefaultPlan(@Nonnull final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan) {
        visit(firstOrDefaultPlan.getChild());
        return pipe().addKeyword("DEFAULT").addWhitespace()
                .addNested(firstOrDefaultPlan.getOnEmptyResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitDefaultOnEmptyPlan(@Nonnull final RecordQueryDefaultOnEmptyPlan defaultOnEmptyPlan) {
        visit(defaultOnEmptyPlan.getChild());
        return pipe().addKeyword("ON").addWhitespace().addKeyword("EMPTY").addWhitespace()
                .addNested(defaultOnEmptyPlan.getOnEmptyResultValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitFlatMapPlan(@Nonnull final RecordQueryFlatMapPlan flatMapPlan) {
        final var outerQuantifier = flatMapPlan.getOuterQuantifier();
        visit(outerQuantifier.getRangesOverPlan());
        pipe().addKeyword("FLATMAP").addWhitespace().addAliasDefinition(outerQuantifier.getAlias()).addWhitespace()
                .addToString("->").addWhitespace().addOpeningBrace().addLinebreakOrWhitespace();
        final var innerQuantifier = flatMapPlan.getInnerQuantifier();
        return visit(innerQuantifier.getRangesOverPlan())
                .addWhitespace().addKeyword("AS").addWhitespace().addAliasDefinition(innerQuantifier.getAlias())
                .addLinebreakOrWhitespace().addKeyword("RETURN").addWhitespace()
                .addNested(flatMapPlan.getResultValue().explain().getExplainTokens())
                .addWhitespace().addClosingBrace();
    }

    @Nonnull
    private ExplainTokens visitInJoinPlan(@Nonnull final RecordQueryInJoinPlan inJoinPlan) {
        final var inSource = inJoinPlan.getInSource();
        final var isCorrelation = Bindings.Internal.CORRELATION.isOfType(inSource.getBindingName());
        final var bindingName =
                isCorrelation
                ? Bindings.Internal.CORRELATION.identifier(inSource.getBindingName())
                : inSource.getBindingName();

        addOpeningSquareBracket().addOptionalWhitespace().addNested(inSource.explain().getExplainTokens())
                .addOptionalWhitespace().addClosingSquareBracket();
        pipe().addKeyword("INJOIN").addWhitespace();
        if (isCorrelation) {
            addAliasDefinition(CorrelationIdentifier.of(bindingName));
        } else {
            addToString(bindingName);
        }
        addWhitespace().addToString("->").addWhitespace().addOpeningBrace().addWhitespace();
        return visit(inJoinPlan.getChild()).addWhitespace().addClosingBrace();
    }

    @Nonnull
    @Override
    public ExplainTokens visitInComparandJoinPlan(@Nonnull final RecordQueryInComparandJoinPlan inComparandJoinPlan) {
        return visitInJoinPlan(inComparandJoinPlan);
    }

    @Nonnull
    @Override
    public ExplainTokens visitMultiIntersectionOnValuesPlan(@Nonnull final RecordQueryMultiIntersectionOnValuesPlan multiIntersectionOnValuesPlan) {
        visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∩").addWhitespace(),
                multiIntersectionOnValuesPlan.getChildren());
        final var compareByExplainTokens = new ExplainTokens().addWhitespace().addKeyword("COMPARE")
                .addWhitespace().addKeyword("BY").addWhitespace()
                .addNested(multiIntersectionOnValuesPlan.getComparisonKeyFunction().explain().getExplainTokens());
        addNested(ExplainLevel.SOME_DETAILS, compareByExplainTokens);

        final List<? extends Quantifier> quantifiers = multiIntersectionOnValuesPlan.getQuantifiers();
        final var withExplainTokens =
                new ExplainTokens().addWhitespace().addKeyword("WITH").addWhitespace()
                        .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                                () -> quantifiers.stream()
                                        .map(quantifier -> new ExplainTokens()
                                                .addAliasDefinition(quantifier.getAlias()))
                                        .iterator());
        addNested(ExplainLevel.SOME_DETAILS, withExplainTokens);
        final var returnExplainTokens = new ExplainTokens().addWhitespace().addKeyword("RETURN")
                .addWhitespace().addNested(multiIntersectionOnValuesPlan.getResultValue().explain().getExplainTokens());
        return addNested(ExplainLevel.SOME_DETAILS, returnExplainTokens);
    }

    @Nonnull
    @Override
    public ExplainTokens visitInParameterJoinPlan(@Nonnull final RecordQueryInParameterJoinPlan inParameterJoinPlan) {
        return visitInJoinPlan(inParameterJoinPlan);
    }

    @Nonnull
    private ExplainTokens visitInUnionPlan(@Nonnull final RecordQueryInUnionPlan inUnionPlan) {
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

        addOpeningSquareBracket().addOptionalWhitespace()
                .addSequence(() -> new ExplainTokens().addWhitespace().addToString("⋈").addWhitespace(),
                        inSourcesBuilder.build())
                .addOptionalWhitespace().addClosingSquareBracket()
                .addWhitespace().addKeyword("INUNION").addWhitespace()
                .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(), bindingsBuilder.build())
                .addWhitespace().addToString("->").addWhitespace()
                .addOpeningBrace().addWhitespace();
        visit(inUnionPlan.getChild()).addWhitespace().addClosingBrace();
        return addWhitespace().addKeyword("COMPARE").addWhitespace().addKeyword("BY").addWhitespace()
                .addNested(ExplainLevel.SOME_DETAILS,
                        inUnionPlan.getComparisonKeyFunction().explain().getExplainTokens());
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
        addKeyword("ISCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        return addNested(indexDetails(indexPlan)).addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitRecursiveUnionPlan(@Nonnull final RecordQueryRecursiveUnionPlan recursiveUnionPlan) {
        Verify.verify(recursiveUnionPlan.getChildren().size() == 2);
        addKeyword("RUNION").addWhitespace()
                .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        new ExplainTokens().addAliasDefinition(recursiveUnionPlan.getTempTableScanAlias()),
                        new ExplainTokens().addAliasDefinition(recursiveUnionPlan.getTempTableInsertAlias())
                                .addWhitespace().addOpeningBrace().addLinebreakOrWhitespace()
                                .addKeyword("INITIAL").addWhitespace().addOpeningBrace().addLinebreakOrWhitespace());
        visit(recursiveUnionPlan.getChildren().get(0)).addWhitespace().addClosingBrace().addLinebreakOrWhitespace();
        addKeyword("RECURSIVE").addWhitespace().addWhitespace().addOpeningBrace().addLinebreakOrWhitespace();
        return visit(recursiveUnionPlan.getChildren().get(1)).addWhitespace().addClosingBrace()
                .addOptionalWhitespace().addClosingBrace();
    }

    @Nonnull
    @Override
    public ExplainTokens visitInsertPlan(@Nonnull final RecordQueryInsertPlan insertPlan) {
        // TODO maybe explain the coercion tree on ALL_DETAILS
        visit(insertPlan.getChild());
        return pipe().addKeyword("INSERT").addWhitespace().addKeyword("INTO").addWhitespace()
                .addIdentifier(insertPlan.getTargetRecordType());
    }

    @Nonnull
    @Override
    public ExplainTokens visitTableFunctionPlan(@Nonnull final RecordQueryTableFunctionPlan tableFunctionPlan) {
        return addKeyword("TF").addWhitespace()
                .addNested(tableFunctionPlan.getValue().explain().getExplainTokens());
    }

    @Nonnull
    @Override
    public ExplainTokens visitTempTableInsertPlan(@Nonnull final TempTableInsertPlan tempTableInsertPlan) {
        // TODO provide proper explain
        visit(tempTableInsertPlan.getChild());
        return pipe().addKeyword("INSERT").addWhitespace().addKeyword("INTO").addWhitespace().addKeyword("TEMP")
                .addWhitespace().addNested(tempTableInsertPlan.getTempTableReferenceValue().explain()
                        .getExplainTokens());
    }

    @Nonnull
    private ExplainTokens visitIntersectionPlan(@Nonnull final RecordQueryIntersectionPlan intersectionPlan) {
        visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∩").addWhitespace(),
                intersectionPlan.getChildren());
        final var compareByExplainTokens = new ExplainTokens().addWhitespace().addKeyword("COMPARE")
                .addWhitespace().addKeyword("BY").addWhitespace()
                .addNested(intersectionPlan.getComparisonKeyFunction().explain().getExplainTokens());
        return addNested(ExplainLevel.SOME_DETAILS, compareByExplainTokens);
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
        return addKeyword("BYKEYS").addWhitespace()
                .addToString(loadByKeysPlan.getKeysSource());
    }

    @Nonnull
    @Override
    public ExplainTokens visitMapPlan(@Nonnull final RecordQueryMapPlan mapPlan) {
        visit(mapPlan.getChild());
        return pipe().addKeyword("MAP").addWhitespace().addPush()
                .addCurrentAliasDefinition(mapPlan.getInner().getAlias())
                .addNested(mapPlan.getResultValue().explain().getExplainTokens()).addPop();
    }

    @Nonnull
    @Override
    public ExplainTokens visitPredicatesFilterPlan(@Nonnull final RecordQueryPredicatesFilterPlan predicatesFilterPlan) {
        visit(predicatesFilterPlan.getChild());
        return pipe().addKeyword("FILTER").addWhitespace().addPush()
                .addCurrentAliasDefinition(predicatesFilterPlan.getInner().getAlias())
                .addNested(predicatesFilterPlan.getConjunctedPredicate().explain().getExplainTokens()).addPop();
    }

    @Nonnull
    @Override
    public ExplainTokens visitRangePlan(@Nonnull RecordQueryRangePlan element) {
        return addKeyword("RANGE").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                .addToString(element.getExclusiveLimitValue())
                .addOptionalWhitespace().addClosingParen();
    }

    @Nonnull
    @Override
    public ExplainTokens visitScanPlan(@Nonnull final RecordQueryScanPlan scanPlan) {
        final var scanComparisons = scanPlan.getScanComparisons();
        final var tupleRange = scanComparisons.toTupleRangeWithoutContext();
        addKeyword("SCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
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
        addKeyword("SRANK").addWhitespace().addToStrings(scoreForRankPlan.getRanks());
        pipe();
        return visit(scoreForRankPlan.getChild());
    }

    @Nonnull
    @Override
    public ExplainPlanVisitor visitSelectorPlan(@Nonnull final RecordQuerySelectorPlan selectorPlan) {
        addKeyword("SELECTOR").addWhitespace().addKeyword("OF").addWhitespace();
        return visitAndJoin(() -> new ExplainTokens().addCommaAndWhiteSpace(), selectorPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitStreamingAggregationPlan(@Nonnull final RecordQueryStreamingAggregationPlan streamingAggregationPlan) {
        visit(streamingAggregationPlan.getChild());
        pipe().addKeyword("AGG").addWhitespace().addPush()
                .addCurrentAliasDefinition(streamingAggregationPlan.getInner().getAlias())
                .addNested(streamingAggregationPlan.getAggregateValue().explain().getExplainTokens());
        final var groupingValue = streamingAggregationPlan.getGroupingValue();
        if (groupingValue != null) {
            return addWhitespace().addKeyword("GROUP").addWhitespace().addKeyword("BY").addWhitespace()
                    .addNested(groupingValue.explain().getExplainTokens());
        }
        addPop();
        return this;
    }

    @Nonnull
    @Override
    public ExplainTokens visitTextIndexPlan(@Nonnull final RecordQueryTextIndexPlan textIndexPlan) {
        addKeyword("TISCAN").addOptionalWhitespace().addOpeningParen().addOptionalWhitespace();
        textIndexDetails(textIndexPlan);
        return addOptionalWhitespace().addClosingParen();
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nonnull
    public ExplainTokens textIndexDetails(@Nonnull final RecordQueryTextIndexPlan textIndexPlan) {
        final TextScan textScan = textIndexPlan.getTextScan();
        addToString(textScan.getIndex().getName()).addCommaAndWhiteSpace();
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
        return this;
    }

    @Nonnull
    @Override
    public ExplainTokens visitTypeFilterPlan(@Nonnull final RecordQueryTypeFilterPlan typeFilterPlan) {
        visit(typeFilterPlan.getChild());
        return pipe().addKeyword("TFILTER").addWhitespace()
                .addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        () -> typeFilterPlan.getRecordTypes()
                                .stream()
                                .map(recordType -> new ExplainTokens().addIdentifier(recordType))
                                .iterator());
    }

    @Nonnull
    @Override
    public ExplainTokens visitRecursivePlan(@Nonnull final RecordQueryRecursivePlan recursivePlan) {
        Verify.verify(recursivePlan.getChildren().size() == 2);
        addKeyword("RUNION-DFS").addWhitespace();
        final var priorValueCorrelation = new ExplainTokens().addAliasDefinition(recursivePlan.getPriorValueCorrelation());
        addNested(ExplainLevel.ALL_DETAILS, priorValueCorrelation).addWhitespace();
        addOpeningBrace().addWhitespace();
        visit(recursivePlan.getChildren().get(0)).addWhitespace();
        addClosingBrace().addLinebreakOrWhitespace();
        addOpeningBrace().addWhitespace();
        addKeyword("RECURSIVE").addWhitespace();
        return visit(recursivePlan.getChildren().get(1)).addWhitespace()
                .addOptionalWhitespace().addClosingBrace();
    }

    @Nonnull
    private ExplainTokens visitUnionPlan(@Nonnull final RecordQueryUnionPlan unionPlan) {
        visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("∪").addWhitespace(),
                unionPlan.getChildren());

        final var compareByExplainTokens = new ExplainTokens().addWhitespace().addKeyword("COMPARE")
                .addWhitespace().addKeyword("BY").addWhitespace()
                .addNested(unionPlan.getComparisonKeyFunction().explain().getExplainTokens());
        return addNested(ExplainLevel.SOME_DETAILS, compareByExplainTokens);
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
        return pipe().addKeyword("DISTINCT").addWhitespace().addKeyword("BY").addWhitespace()
                .addToString(ExplainLevel.SOME_DETAILS, unorderedDistinctPlan.getComparisonKey());
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
        visit(unorderedPrimaryKeyDistinctPlan.getChild());
        return pipe().addKeyword("DISTINCT").addWhitespace().addKeyword("BY").addWhitespace().addKeyword("PK");
    }

    @Nonnull
    @Override
    public ExplainTokens visitUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unorderedUnionPlan) {
        return visitAndJoin(() -> new ExplainTokens().addWhitespace().addToString("⊎").addWhitespace(),
                unorderedUnionPlan.getChildren());
    }

    @Nonnull
    @Override
    public ExplainTokens visitUpdatePlan(@Nonnull final RecordQueryUpdatePlan updatePlan) {
        // TODO explain with coercion and update tries in ALL_DETAILS
        visit(updatePlan.getChild());
        return pipe().addKeyword("UPDATE").addWhitespace().addIdentifier(updatePlan.getTargetRecordType());
    }

    @Nonnull
    @Override
    public ExplainTokens visitDamPlan(@Nonnull final RecordQueryDamPlan damPlan) {
        visit(damPlan.getChild());
        return pipe().addKeyword("DAM");
    }

    @Nonnull
    @Override
    public ExplainTokens visitSortPlan(@Nonnull final RecordQuerySortPlan sortPlan) {
        visit(sortPlan.getChild());
        return pipe().addKeyword("SORT").addWhitespace().addKeyword("BY").addWhitespace()
                .addToString(sortPlan.getKey());
    }

    @Nonnull
    @Override
    public ExplainTokens visitTempTableScanPlan(@Nonnull final TempTableScanPlan tempTableScanPlan) {
        return addKeyword("TEMP").addWhitespace().addKeyword("SCAN").addWhitespace()
                .addNested(tempTableScanPlan.getTempTableReferenceValue().explain().getExplainTokens());
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
        if (element instanceof RecordQueryPlanWithExplain) {
            addNested(((RecordQueryPlanWithExplain)element).explain().getExplainTokens());
            return this;
        }

        throw new RecordCoreException("no default implementation");
    }

    @Nonnull
    public static ExplainTokens indexDetails(@Nonnull final RecordQueryIndexPlan indexPlan) {
        final var resultExplainTokens = new ExplainTokens();
        final IndexScanParameters scanParameters = indexPlan.getScanParameters();
        resultExplainTokens.addIdentifier(indexPlan.getIndexName()).addWhitespace(ExplainLevel.SOME_DETAILS)
                .addNested(ExplainLevel.SOME_DETAILS, scanParameters.explain().getExplainTokens());
        final var scanPropertiesExplainTokens = new ExplainTokens();
        if (!IndexScanType.BY_VALUE.equals(scanParameters.getScanType())) {
            scanPropertiesExplainTokens.addWhitespace().addKeyword(scanParameters.getScanType().toString());
        }
        if (indexPlan.isReverse()) {
            scanPropertiesExplainTokens.addWhitespace().addKeyword("REVERSE");
        }
        resultExplainTokens.addNested(ExplainLevel.SOME_DETAILS, scanPropertiesExplainTokens);
        return resultExplainTokens;
    }

    @Nonnull
    public static String prettyExplain(@Nonnull final RecordQueryPlan plan, final int explainLevel) {
        final var visitor = new ExplainPlanVisitor(Integer.MAX_VALUE);
        return visitor.visit(plan).render(explainLevel,
                new PrettyExplainFormatter(ExplainSelfContainedSymbolMap::new, true),
                Integer.MAX_VALUE).toString();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static String toStringForDebugging(@Nonnull final RecordQueryPlan plan) {
        return toStringForDebugging(plan, ExplainLevel.ALL_DETAILS, Integer.MAX_VALUE);
    }

    @Nonnull
    public static String toStringForDebugging(@Nonnull final RecordQueryPlan plan, final int explainLevel,
                                              final int maxSize) {
        final var visitor = new ExplainPlanVisitor(maxSize);
        final var explainTokens = visitor.visit(plan);
        return explainTokens.render(explainLevel,
                new DefaultExplainFormatter(DefaultExplainSymbolMap::new), maxSize).toString();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static String toStringForExternalExplain(@Nonnull final RecordQueryPlan plan) {
        return toStringForExternalExplain(plan, ExplainLevel.ALL_DETAILS, Integer.MAX_VALUE);
    }

    @Nonnull
    public static String toStringForExternalExplain(@Nonnull final RecordQueryPlan plan, final int maxExplainLevel,
                                                    final int maxSize) {
        final var visitor = new ExplainPlanVisitor(maxSize);
        final var explainTokens = visitor.visit(plan);
        int level;
        if (maxSize < Integer.MAX_VALUE) {
            int i;
            for (i = ExplainLevel.ALL_DETAILS; i < maxExplainLevel; i++) {
                if (explainTokens.getMaxLength(i) <= maxSize) {
                    break;
                }
            }
            level = i;
        } else {
            level = ExplainLevel.ALL_DETAILS;
        }

        return explainTokens.render(level, new DefaultExplainFormatter(ExplainSelfContainedSymbolMap::new), maxSize)
                .toString();
    }

    @Nonnull
    public static String prettyExplain(@Nonnull final RecordQueryPlan plan) {
        return ExplainPlanVisitor.prettyExplain(plan, ExplainLevel.ALL_DETAILS);
    }
}
