/*
 * FilterMatcherWithComponent.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A specialized Hamcrest matcher that recognizes both {@link RecordQueryFilterPlan}s and the {@link QueryComponent}s
 * that they use <em>and</em> {@link RecordQueryPredicatesFilterPlan}s and the {@link QueryPredicate}s that they use.
 * This is designed to support the current {@link com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest}
 * infrastructure where we run exactly the same unit tests using both the
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} (which produces {@link RecordQueryFilterPlan}s)
 * and the {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner} (which produces
 * {@link RecordQueryPredicatesFilterPlan}).
 */
public class FilterMatcherWithComponent extends PlanMatcherWithChild {
    @Nonnull
    private final QueryComponent component;

    public FilterMatcherWithComponent(@Nonnull QueryComponent component, @Nonnull Matcher<RecordQueryPlan> childMatcher) {
        super(childMatcher);
        this.component = component;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        if (plan instanceof RecordQueryFilterPlan) {
            return component.equals(((RecordQueryFilterPlan)plan).getConjunctedFilter()) && super.matchesSafely(plan);
        } else if (plan instanceof RecordQueryPredicatesFilterPlan) {
            // todo make more robust as this will currently only work with the simplest of all cases

            // we lazily convert the given component to a predicate and let semantic equals establish equality
            // under the given equivalence: baseAlias <-> planBaseAlias
            final QueryPredicate predicate = AndPredicate.and(((RecordQueryPredicatesFilterPlan)plan).getPredicates());
            
            if (predicate instanceof PredicateWithValue) {
                final Set<CorrelationIdentifier> predicateCorrelatedTo = predicate.getCorrelatedTo();
                if (predicateCorrelatedTo.size() != 1) {
                    return false;
                }
                final CorrelationIdentifier singlePredicateAlias = Iterables.getOnlyElement(predicateCorrelatedTo);
                final Quantifier planBaseQuantifier = Iterables.getOnlyElement(plan.getQuantifiers());
                final Quantifier.ForEach expandBaseQuantifier = Quantifier.forEach(planBaseQuantifier.getRangesOver(), planBaseQuantifier.getAlias());
                final GraphExpansion graphExpansion =
                        component.expand(expandBaseQuantifier, () -> {
                            throw new UnsupportedOperationException();
                        });
                return predicate.semanticEquals(graphExpansion.asAndPredicate(), AliasMap.ofAliases(planBaseQuantifier.getAlias(), singlePredicateAlias))
                       && super.matchesSafely(plan);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Filter(" + component + "; ");
        super.describeTo(description);
        description.appendText(")");
    }
}
