/*
 * QuantifierMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Matches a subclass of {@link PartialMatch} and a given matcher against the children.
 * @param <T> the type of {@link PartialMatch} to match against
 */
public class PartialMatchMatcher<T extends PartialMatch> extends TypeMatcher<T> {
    private static final ExpressionMatcher<PartialMatch> CURRENT_PARTIAL_MATCHES_BINDING_KEY = ExpressionMatcher.uniqueBindingKey();

    /**
     * Private constructor. Use static factory methods.
     * @param partialMatchClass the class of the partial match
     * @param childrenMatcher matcher for children
     */
    private PartialMatchMatcher(@Nonnull final Class<? extends T> partialMatchClass,
                                @Nonnull final ExpressionChildrenMatcher childrenMatcher) {
        super(partialMatchClass, childrenMatcher);
    }

    /**
     * Matches any {@link PartialMatch}.
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static PartialMatchMatcher<PartialMatch> any() {
        return new PartialMatchMatcher<>(PartialMatch.class, AnyChildrenMatcher.ANY);
    }

    /**
     * Matches any {@link PartialMatch} that is complete.
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static PartialMatchMatcher<PartialMatch> completeMatch() {
        return new PartialMatchMatcher<PartialMatch>(PartialMatch.class, AnyChildrenMatcher.ANY) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> matchWith(final @Nonnull PlannerBindings outerBindings, @Nonnull final PartialMatch partialMatch, @Nonnull final List<? extends Bindable> children) {
                if (partialMatch.getMatchCandidate().getTraversal().getRootReference() != partialMatch.getCandidateRef()) {
                    return Stream.empty();
                }
                return super.matchWith(outerBindings, partialMatch, children);
            }
        };
    }

    /**
     * Matches any {@link PartialMatch} that is not yet complete.
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static PartialMatchMatcher<PartialMatch> incompleteMatch() {
        return new PartialMatchMatcher<PartialMatch>(PartialMatch.class, AnyChildrenMatcher.ANY) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> matchWith(final @Nonnull PlannerBindings outerBindings, @Nonnull final PartialMatch partialMatch, @Nonnull final List<? extends Bindable> children) {
                if (partialMatch.getMatchCandidate().getTraversal().getRootReference() == partialMatch.getCandidateRef()) {
                    return Stream.empty();
                }
                return super.matchWith(outerBindings, partialMatch, children);
            }
        };
    }

    /**
     * Matches any {@link PartialMatch} that is complete.
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static PartialMatchMatcher<PartialMatch> completeMatchOnExpression() {
        return new PartialMatchMatcher<PartialMatch>(PartialMatch.class, AnyChildrenMatcher.ANY) {
            @Nonnull
            @Override
            public Stream<PlannerBindings> matchWith(final @Nonnull PlannerBindings outerBindings, @Nonnull final PartialMatch partialMatch, @Nonnull final List<? extends Bindable> children) {
                if (partialMatch.getMatchCandidate().getTraversal().getRootReference() != partialMatch.getQueryRef()) {
                    return Stream.empty();
                }
                return super.matchWith(outerBindings, partialMatch, children);
            }
        };
    }

    public static ExpressionMatcher<PartialMatch> expressionWithCurrentPartialMatches() {
        return CURRENT_PARTIAL_MATCHES_BINDING_KEY;
    }
}
