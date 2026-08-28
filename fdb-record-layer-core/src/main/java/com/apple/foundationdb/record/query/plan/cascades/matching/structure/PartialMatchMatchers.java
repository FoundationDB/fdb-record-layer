/*
 * PartialMatchMatchers.java
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

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.AggregateIndexMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;

import java.util.stream.Stream;

/**
 * Matchers of {@link PartialMatch}.
 */
public class PartialMatchMatchers {
    private PartialMatchMatchers() {
        // do not instantiate
    }

    /**
     * Matches any {@link PartialMatch}.
     * @return a matcher matching any partial match
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> anyPartialMatch() {
        return new TypedMatcher<>(PartialMatch.class);
    }

    /**
     * Matches any {@link PartialMatch} that is complete.
     * @return a matcher matching any partial match that is complete
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> completeMatch() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(final RecordQueryPlannerConfiguration plannerConfiguration, final PlannerBindings outerBindings, final PartialMatch in) {
                return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                        .flatMap(bindings -> {
                            if (in.getMatchCandidate().getTraversal().getRootReference() != in.getCandidateRef()) {
                                return Stream.empty();
                            } else {
                                return Stream.of(bindings);
                            }
                        });
            }
        };
    }

    /**
     * Matches any {@link PartialMatch} that is not yet complete.
     * @return a matcher matching any partial match that is incomplete
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> incompleteMatch() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(final RecordQueryPlannerConfiguration plannerConfiguration, final PlannerBindings outerBindings, final PartialMatch in) {
                return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                        .flatMap(bindings -> {
                            if (in.getMatchCandidate().getTraversal().getRootReference() == in.getCandidateRef()) {
                                return Stream.empty();
                            } else {
                                return Stream.of(bindings);
                            }
                        });
            }
        };
    }

    /**
     * Matches any {@link PartialMatch} that is a match with a
     * {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate} that is backed by data structure that
     * understands the concept of a primary key. Those match candidates are primary scan candidates, value indexes,
     * and windowed indexes.
     * @return a matcher matching a {@link PartialMatch} whose
     *         {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate} implements
     *         {@link WithPrimaryKeyMatchCandidate}.
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> matchingWithPrimaryKeyMatchCandidate() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(final RecordQueryPlannerConfiguration plannerConfiguration,
                                                             final PlannerBindings outerBindings, final PartialMatch in) {
                return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                        .filter(bindings -> in.getMatchCandidate() instanceof WithPrimaryKeyMatchCandidate);
            }
        };
    }

    /**
     * Matches any {@link PartialMatch} that is a match with an {@link AggregateIndexMatchCandidate}.
     * @return a matcher matching any partial match whose
     *         {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate} is of type
     *         {@link AggregateIndexMatchCandidate}.
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> matchingAggregateIndexMatchCandidate() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(final RecordQueryPlannerConfiguration plannerConfiguration,
                                                             final PlannerBindings outerBindings, final PartialMatch in) {
                return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                        .filter(bindings -> in.getMatchCandidate() instanceof AggregateIndexMatchCandidate);
            }
        };
    }
}
