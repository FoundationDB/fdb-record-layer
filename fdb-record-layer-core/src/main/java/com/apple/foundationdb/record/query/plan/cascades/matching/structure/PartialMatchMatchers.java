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
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;

import javax.annotation.Nonnull;
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
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> anyPartialMatch() {
        return new TypedMatcher<>(PartialMatch.class);
    }

    /**
     * Matches any {@link PartialMatch} that is complete.
     * @return a matcher matching any partial match that is complete
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> completeMatch() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final PartialMatch in) {
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
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static BindingMatcher<PartialMatch> incompleteMatch() {
        return new TypedMatcher<>(PartialMatch.class) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final PartialMatch in) {
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
}
