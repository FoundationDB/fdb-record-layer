/*
 * SyntheticPlanMatchers.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Matchers for {@link SyntheticRecordPlanner} outputs,
 * such as {@link SyntheticRecordPlan} and {@link SyntheticRecordFromStoredRecordPlan}.
 */
public class SyntheticPlanMatchers {
    public static Matcher<SyntheticRecordPlan> syntheticRecordScan(@Nonnull Matcher<RecordQueryPlan> seedMatcher,
                                                                   @Nonnull Matcher<SyntheticRecordFromStoredRecordPlan> fromSeedMatcher) {
        return new SyntheticRecordScanPlanMatcher(seedMatcher, fromSeedMatcher);
    }

    public static Matcher<SyntheticRecordFromStoredRecordPlan> joinedRecord(@Nonnull List<Matcher<RecordQueryPlan>> planMatchers) {
        return new JoinedRecordPlanMatcher(planMatchers);
    }

    public static Matcher<SyntheticRecordFromStoredRecordPlan> syntheticRecordConcat(@Nonnull List<Matcher<SyntheticRecordFromStoredRecordPlan>> planMatchers) {
        return new SyntheticRecordConcatPlanMatcher(planMatchers);
    }

    public static Matcher<SyntheticRecordFromStoredRecordPlan> syntheticRecordByType(@Nonnull Map<String, Matcher<SyntheticRecordFromStoredRecordPlan>> subMatchers) {
        return new SyntheticRecordByTypePlanMatcher(subMatchers);
    }

    /**
     * Match {@link SyntheticRecordScanPlan} as {@link SyntheticRecordPlan}.
     */
    public static class SyntheticRecordScanPlanMatcher extends TypeSafeMatcher<SyntheticRecordPlan> {
        @Nonnull
        private final Matcher<RecordQueryPlan> seedMatcher;
        @Nonnull
        private final Matcher<SyntheticRecordFromStoredRecordPlan> fromSeedMatcher;

        public SyntheticRecordScanPlanMatcher(@Nonnull Matcher<RecordQueryPlan> seedMatcher,
                                              @Nonnull Matcher<SyntheticRecordFromStoredRecordPlan> fromSeedMatcher) {
            this.seedMatcher = seedMatcher;
            this.fromSeedMatcher = fromSeedMatcher;
        }

        @Override
        public boolean matchesSafely(@Nonnull SyntheticRecordPlan plan) {
            if (!(plan instanceof SyntheticRecordScanPlan)) {
                return false;
            }
            SyntheticRecordScanPlan syntheticRecordScanPlan = (SyntheticRecordScanPlan)plan;
            return seedMatcher.matches(syntheticRecordScanPlan.getSeedPlan()) &&
                   fromSeedMatcher.matches(syntheticRecordScanPlan.getFromSeedPlan());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("seed=(");
            seedMatcher.describeTo(description);
            description.appendText(",then=(");
            fromSeedMatcher.describeTo(description);
            description.appendText(")");
        }
    }

    /**
     * Match {@link JoinedRecordPlan} as {@link SyntheticRecordFromStoredRecordPlan}.
     */
    public static class JoinedRecordPlanMatcher extends TypeSafeMatcher<SyntheticRecordFromStoredRecordPlan> {
        @Nonnull
        private final List<Matcher<RecordQueryPlan>> planMatchers;

        public JoinedRecordPlanMatcher(@Nonnull List<Matcher<RecordQueryPlan>> planMatchers) {
            this.planMatchers = planMatchers;
        }

        @Override
        public boolean matchesSafely(@Nonnull SyntheticRecordFromStoredRecordPlan plan) {
            if (!(plan instanceof JoinedRecordPlan)) {
                return false;
            }
            JoinedRecordPlan joinedPlan = (JoinedRecordPlan)plan;
            List<RecordQueryPlan> queries = joinedPlan.getQueries();
            if (queries.size() != planMatchers.size()) {
                return false;
            }
            for (int i = 0; i < queries.size(); i++) {
                if (!planMatchers.get(i).matches(queries.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendList("plans=(", ", ", ")", planMatchers);
        }
    }

    /**
     * Match {@link SyntheticRecordConcatPlan} as {@link SyntheticRecordFromStoredRecordPlan}.
     */
    public static class SyntheticRecordConcatPlanMatcher extends TypeSafeMatcher<SyntheticRecordFromStoredRecordPlan> {
        @Nonnull
        private final List<Matcher<SyntheticRecordFromStoredRecordPlan>> planMatchers;

        public SyntheticRecordConcatPlanMatcher(@Nonnull List<Matcher<SyntheticRecordFromStoredRecordPlan>> planMatchers) {
            this.planMatchers = planMatchers;
        }

        @Override
        public boolean matchesSafely(@Nonnull SyntheticRecordFromStoredRecordPlan plan) {
            if (!(plan instanceof SyntheticRecordConcatPlan)) {
                return false;
            }
            SyntheticRecordConcatPlan concatPlan = (SyntheticRecordConcatPlan)plan;
            List<SyntheticRecordFromStoredRecordPlan> subPlans = concatPlan.getSubPlans();
            if (subPlans.size() != planMatchers.size()) {
                return false;
            }
            for (int i = 0; i < subPlans.size(); i++) {
                if (!planMatchers.get(i).matches(subPlans.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendList("plans=(", ", ", ")", planMatchers);
        }
    }

    /**
     * Match {@link SyntheticRecordByTypePlan} as {@link SyntheticRecordFromStoredRecordPlan}.
     */
    public static class SyntheticRecordByTypePlanMatcher extends TypeSafeMatcher<SyntheticRecordFromStoredRecordPlan> {
        @Nonnull
        private final Map<String, Matcher<SyntheticRecordFromStoredRecordPlan>> subMatchers;

        public SyntheticRecordByTypePlanMatcher(@Nonnull Map<String, Matcher<SyntheticRecordFromStoredRecordPlan>> subMatchers) {
            this.subMatchers = subMatchers;
        }

        @Override
        public boolean matchesSafely(@Nonnull SyntheticRecordFromStoredRecordPlan plan) {
            if (!(plan instanceof SyntheticRecordByTypePlan)) {
                return false;
            }
            SyntheticRecordByTypePlan byTypePlan = (SyntheticRecordByTypePlan)plan;
            Map<String, SyntheticRecordFromStoredRecordPlan> subPlans = byTypePlan.getSubPlans();
            if (!subPlans.keySet().equals(subMatchers.keySet())) {
                return false;
            }
            for (Map.Entry<String, SyntheticRecordFromStoredRecordPlan> entry : subPlans.entrySet()) {
                if (!subMatchers.get(entry.getKey()).matches(entry.getValue())) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("plans=(");
            boolean first = true;
            for (Map.Entry<String, Matcher<SyntheticRecordFromStoredRecordPlan>> entry : subMatchers.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    description.appendText(", ");
                }
                description.appendText(entry.getKey());
                description.appendText("=");
                entry.getValue().describeTo(description);
            }
        }
    }
}
