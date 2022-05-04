/*
 * RelationalExpressionMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.getTopExpressionReferenceMatcher;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for descendants of {@link RelationalExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public class RelationalExpressionMatchers {
    private RelationalExpressionMatchers() {
        // do not instantiate
    }

    @Nonnull
    public static BindingMatcher<RelationalExpression> isTopExpression() {
        return ContainsExpressionInReferenceMatcher.containsExpressionInReference(getTopExpressionReferenceMatcher());
    }

    public static BindingMatcher<RelationalExpression> anyExpression() {
        return typed(RelationalExpression.class);
    }

    public static <R extends RelationalExpression> TypedMatcher<R> ofType(@Nonnull final Class<R> bindableClass) {
        return typed(bindableClass);
    }

    public static <R extends RelationalExpression> BindingMatcher<R> ofType(@Nonnull final Class<R> bindableClass,
                                                                            @Nonnull final BindingMatcher<R> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicates(@Nonnull final Class<R> bindableClass,
                                                                                                                                                    @Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(RelationalExpressionWithPredicates::getPredicates, name -> "predicates(" + name + ")"),
                downstream);
    }

    public static <R extends RelationalExpression, C extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                              @Nonnull final BindingMatcher<C> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                downstream);
    }

    public static <R extends RelationalExpressionWithPredicates, C1 extends Collection<? extends QueryPredicate>, C2 extends Collection<? extends Quantifier>> BindingMatcher<R> ofTypeWithPredicatesAndOwning(@Nonnull final Class<R> bindableClass,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C1> downstreamPredicates,
                                                                                                                                                                                                               @Nonnull final BindingMatcher<C2> downstreamQuantifiers) {
        return typedWithDownstream(bindableClass,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(RelationalExpressionWithPredicates.class,
                        ImmutableList.of(
                                typedWithDownstream(bindableClass,
                                        Extractor.of(RelationalExpressionWithPredicates::getPredicates, name -> "predicates(" + name + ")"),
                                        downstreamPredicates),
                                typedWithDownstream(bindableClass,
                                        Extractor.of(RelationalExpression::getQuantifiers, name -> "quantifiers(" + name + ")"),
                                        downstreamQuantifiers))));
    }

    @Nonnull
    public static BindingMatcher<FullUnorderedScanExpression> fullUnorderedScanExpression() {
        return ofTypeOwning(FullUnorderedScanExpression.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<IndexScanExpression> indexScanExpression() {
        return ofTypeOwning(IndexScanExpression.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<LogicalDistinctExpression> logicalDistinctExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalDistinctExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalDistinctExpression> logicalDistinctExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalDistinctExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalFilterExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalFilterExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(@Nonnull final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                  @Nonnull final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(LogicalFilterExpression.class, AnyMatcher.any(downstreamPredicates), AnyMatcher.any(downstreamQuantifiers));
    }

    @Nonnull
    public static BindingMatcher<LogicalFilterExpression> logicalFilterExpression(@Nonnull final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                                  @Nonnull final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(LogicalFilterExpression.class, downstreamPredicates, downstreamQuantifiers);
    }

    @Nonnull
    public static BindingMatcher<LogicalProjectionExpression> logicalProjectionExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalProjectionExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalProjectionExpression> logicalProjectionExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalProjectionExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<LogicalSortExpression> logicalSortExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalSortExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalSortExpression> logicalSortExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalSortExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<LogicalTypeFilterExpression> logicalTypeFilterExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalTypeFilterExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalTypeFilterExpression> logicalTypeFilterExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalTypeFilterExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<LogicalUnionExpression> logicalUnionExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalUnionExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<LogicalUnionExpression> logicalUnionExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(LogicalUnionExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<PrimaryScanExpression> primaryScanExpression() {
        return ofTypeOwning(PrimaryScanExpression.class, CollectionMatcher.empty());
    }

    @Nonnull
    public static BindingMatcher<SelectExpression> selectExpression(@Nonnull final BindingMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(SelectExpression.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<SelectExpression> selectExpression(@Nonnull final CollectionMatcher<? extends Quantifier> downstream) {
        return ofTypeOwning(SelectExpression.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<SelectExpression> selectExpression(@Nonnull final BindingMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                    @Nonnull final BindingMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(SelectExpression.class, AnyMatcher.any(downstreamPredicates), AnyMatcher.any(downstreamQuantifiers));
    }

    @Nonnull
    public static BindingMatcher<SelectExpression> selectExpression(@Nonnull final CollectionMatcher<? extends QueryPredicate> downstreamPredicates,
                                                                    @Nonnull final CollectionMatcher<? extends Quantifier> downstreamQuantifiers) {
        return ofTypeWithPredicatesAndOwning(SelectExpression.class, downstreamPredicates, downstreamQuantifiers);
    }

    @Nonnull
    public static BindingMatcher<ExplodeExpression> explodeExpression() {
        return ofTypeOwning(ExplodeExpression.class, CollectionMatcher.empty());
    }
}
