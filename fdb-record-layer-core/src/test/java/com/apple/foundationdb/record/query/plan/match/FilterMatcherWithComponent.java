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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicateFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;

/**
 * A specialized Hamcrest matcher that recognizes both {@link RecordQueryFilterPlan}s and the {@link QueryComponent}s
 * that they use <em>and</em> {@link RecordQueryPredicateFilterPlan}s and the {@link QueryPredicate}s that they use.
 * This is designed to support the current {@link com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest}
 * infrastructure where we run exactly the same unit tests using both the
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} (which produces {@link RecordQueryFilterPlan}s)
 * and the {@link com.apple.foundationdb.record.query.plan.temp.CascadesPlanner} (which produces
 * {@link RecordQueryPredicateFilterPlan}).
 *
 * <p>
 * Note that this matcher must store a {@link QueryComponent} rather than a {@link QueryPredicate} or
 * {@code Matcher<QueryComponent>} because it must be able to convert it to an equivalent {@code QueryPredicate} using
 * the {@link QueryComponent#normalizeForPlanner(Source, java.util.List)} method.
 * </p>
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
            return component.equals(((RecordQueryFilterPlan)plan).getFilter()) && super.matchesSafely(plan);
        } else if (plan instanceof RecordQueryPredicateFilterPlan) {
            QueryPredicate predicate = ((RecordQueryPredicateFilterPlan)plan).getFilter();
            return predicate.equals(component.normalizeForPlanner(((RecordQueryPredicateFilterPlan)plan).getBaseSource()))
                    && super.matchesSafely(plan);
        } else {
            return false;
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Filter(" + component.toString() + "; ");
        super.describeTo(description);
        description.appendText(")");
    }
}
