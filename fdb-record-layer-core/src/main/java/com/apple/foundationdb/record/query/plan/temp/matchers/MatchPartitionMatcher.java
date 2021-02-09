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

import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher.someMatching;

/**
 * Matches a subclass of {@link MatchPartition} and a given matcher against the children.
 * @param <T> the type of {@link MatchPartition} to match against
 */
public class MatchPartitionMatcher<T extends MatchPartition> extends TypeMatcher<T> {
    /**
     * Private constructor. Use static factory methods.
     * @param matchPartitionClass the class of the match partition
     * @param childrenMatcher matcher for children
     */
    private MatchPartitionMatcher(@Nonnull final Class<? extends T> matchPartitionClass,
                                  @Nonnull final ExpressionChildrenMatcher childrenMatcher) {
        super(matchPartitionClass, childrenMatcher);
    }

    /**
     * Matches any {@link MatchPartition}.
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static MatchPartitionMatcher<MatchPartition> any() {
        return new MatchPartitionMatcher<>(MatchPartition.class, AnyChildrenMatcher.ANY);
    }

    /**
     * Matches any {@link MatchPartition} satisfying the given {@code partialMatchMatcher} for some of its
     * {@link PartialMatch}es.
     * @param partialMatchMatcher a matcher to be applied to each individual {@link PartialMatch} of the match partition
     * @return a matcher matching any partial match
     */
    @Nonnull
    public static MatchPartitionMatcher<MatchPartition> some(@Nonnull ExpressionMatcher<? extends PartialMatch> partialMatchMatcher) {
        return new MatchPartitionMatcher<>(MatchPartition.class, someMatching(partialMatchMatcher));
    }
}
