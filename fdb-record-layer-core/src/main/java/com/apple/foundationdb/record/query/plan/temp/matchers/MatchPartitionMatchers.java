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
import java.util.Collection;

/**
 * Matches a subclass of {@link MatchPartition} and a given matcher against the children.
 */
public class MatchPartitionMatchers {
    private MatchPartitionMatchers() {
        // do not instantiate
    }

    @Nonnull
    public static <C extends Collection<? extends PartialMatch>> BindingMatcher<MatchPartition> containing(@Nonnull final BindingMatcher<C> downstream) {
        return TypedMatcherWithExtractAndDownstream.of(MatchPartition.class,
                MatchPartition::getPartialMatches,
                downstream);
    }
}
