/*
 * UnionMatcher.java
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

package com.apple.foundationdb.record.query.plan.match;

import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * The union matcher requires two or more child matchers which must match the children of a union
 * in some order.
 */
public class UnionMatcher extends PlanMatcherWithChildren {
    private final Matcher<KeyExpression> comparisonKeyMatcher;

    public UnionMatcher(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers) {
        this(childMatchers, TestHelpers.RealAnythingMatcher.anything());
    }

    public UnionMatcher(@Nonnull List<Matcher<RecordQueryPlan>> childMatchers,
                        @Nonnull Matcher<KeyExpression> comparisonKeyMatcher) {
        super(childMatchers);
        this.comparisonKeyMatcher = comparisonKeyMatcher;
    }

    @Override
    public boolean matchesSafely(@Nonnull RecordQueryPlan plan) {
        return plan instanceof RecordQueryUnionPlan &&
                super.matchesSafely(plan) &&
                comparisonKeyMatcher.matches(((RecordQueryUnionOnKeyExpressionPlan) plan).getComparisonKeyExpression());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Union(");
        super.describeTo(description);
        description.appendText("; comparison key=");
        comparisonKeyMatcher.describeTo(description);
        description.appendText(")");
    }
}
