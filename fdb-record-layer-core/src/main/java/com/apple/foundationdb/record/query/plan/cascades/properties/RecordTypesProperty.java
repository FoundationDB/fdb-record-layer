/*
 * RecordTypesProperty.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers.AliasResolver;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TypeFilterExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A property visitor that determines the set of record type names (as Strings) that a {@link RelationalExpression}
 * could produce. This property is used in determining whether type filters are necessary, among other things.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RecordTypesProperty implements ExpressionProperty<Set<String>> {
    private static final RecordTypesProperty RECORD_TYPES = new RecordTypesProperty();

    private RecordTypesProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RecordTypesVisitor createVisitor() {
        return new RecordTypesVisitor(Optional.empty());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Nonnull
    public Set<String> evaluate(@Nonnull Reference reference) {
        return Objects.requireNonNull(reference.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public Set<String> evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public static RecordTypesProperty recordTypes() {
        return RECORD_TYPES;
    }

    public static class RecordTypesVisitor implements SimpleExpressionVisitor<Set<String>> {
        @Nonnull
        private final Optional<AliasResolver> aliasResolverOptional;

        public RecordTypesVisitor(@Nonnull final AliasResolver aliasResolver) {
            this(Optional.of(aliasResolver));
        }

        public RecordTypesVisitor(@Nonnull final Optional<AliasResolver> aliasResolverOptional) {
            this.aliasResolverOptional = aliasResolverOptional;
        }

        @Nonnull
        @Override
        public Set<String> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Set<String>> childResults) {
            if (expression instanceof RecordQueryScanPlan) {
                final var recordTypesFromExpression = ((RecordQueryScanPlan)expression).getRecordTypes();
                return recordTypesFromExpression == null ? ImmutableSet.of() : recordTypesFromExpression;
            } else if (expression instanceof FullUnorderedScanExpression) {
                return ((FullUnorderedScanExpression)expression).getRecordTypes();
            } else if (expression instanceof RecordQueryIndexPlan) {
                return ((RecordQueryIndexPlan)expression).getMatchCandidateMaybe()
                        .map(MatchCandidate::getQueriedRecordTypeNames)
                        .orElse(ImmutableSet.of());
            } else if (expression instanceof TypeFilterExpression) {
                return Sets.filter(childResults.get(0), ((TypeFilterExpression)expression).getRecordTypes()::contains);
            } else if (childResults.isEmpty()) {
                // try to see if the leaf expression is correlated and follow up the correlations
                final Set<String> recordTypes = Sets.newHashSet();
                for (final CorrelationIdentifier alias : expression.getCorrelatedTo()) {
                    final Set<Quantifier> quantifiers = aliasResolverOptional
                            .map(aliasResolver -> aliasResolver.resolveCorrelationAlias(expression, alias))
                            .orElse(ImmutableSet.of());
                    for (final Quantifier quantifier : quantifiers) {
                        recordTypes.addAll(Objects.requireNonNull(quantifier.getRangesOver().acceptVisitor(this)));
                    }
                }

                //
                // If we use an alias resolver we should find where the correlations come from, if not, everything is
                // best-effort.
                //
                if (aliasResolverOptional.isPresent() && recordTypes.isEmpty()) {
                    throw new RecordCoreException("tried to find record types for a relational expression with no children but case wasn't handled");
                }
                return recordTypes;
            } else {
                int nonNullChildResult = 0;
                Set<String> firstChildResult = null;
                for (Set<String> result : childResults) {
                    if (result != null) {
                        nonNullChildResult++;
                        if (firstChildResult == null) {
                            firstChildResult = result;
                        }
                    }
                }

                if (nonNullChildResult == 1) {
                    return firstChildResult;
                } else {
                    // If we have a single child, then there is a reasonable default for how most relational expressions will
                    // change the set of record types (i.e., they won't change them at all). However, if you have several relational
                    // children (like a union or intersection expression) then we must specify some way to combine them.
                    if (expression instanceof RecordQueryUnionPlan ||
                            expression instanceof RecordQueryUnorderedUnionPlan ||
                            expression instanceof RecordQueryIntersectionPlan ||
                            expression instanceof LogicalUnionExpression ||
                            expression instanceof RecursiveUnionExpression ||
                            expression instanceof SelectExpression ||
                            expression instanceof RecordQueryFlatMapPlan ||
                            expression instanceof RecursiveExpression) {
                        final Set<String> union = new HashSet<>();
                        for (Set<String> childResulSet : childResults) {
                            union.addAll(childResulSet);
                        }
                        return union;
                    } else {
                        throw new RecordCoreException("tried to find record types for a relational expression with multiple " +
                                "relational children, but no combiner was specified");
                    }
                }
            }
        }

        @Nonnull
        @Override
        public Set<String> evaluateAtRef(@Nonnull Reference ref,
                                         @Nonnull List<Set<String>> memberResults) {
            final Set<String> union = new HashSet<>();
            for (Set<String> resultSet : memberResults) {
                union.addAll(resultSet);
            }
            return union;
        }
    }
}
