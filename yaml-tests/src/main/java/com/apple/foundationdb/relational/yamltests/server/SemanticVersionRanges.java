/*
 * SemanticVersionRanges.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.server;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helpers shared by yamsql directives that gate execution on the initial version of the connection, in particular
 * {@code initialVersionAtLeast} and {@code initialVersionLessThan}.
 */
public final class SemanticVersionRanges {

    private SemanticVersionRanges() {
    }

    /**
     * Returns the {@code [v, MAX)} range, meaning versions {@code v} and above.
     * Mirrors the {@code initialVersionAtLeast} directive.
     */
    @Nonnull
    public static Range<SemanticVersion> atLeast(@Nonnull SemanticVersion v) {
        return Range.closedOpen(v, SemanticVersion.max());
    }

    /**
     * The {@code [MIN, v)} range, meaning versions strictly below {@code v}.
     * Mirrors the {@code initialVersionLessThan} directive.
     */
    @Nonnull
    public static Range<SemanticVersion> lessThan(@Nonnull SemanticVersion v) {
        return Range.closedOpen(SemanticVersion.min(), v);
    }

    /**
     * The full {@code [MIN, MAX)} range. This is the implicit range for an entry that declares no version constraint.
     */
    @Nonnull
    public static Range<SemanticVersion> all() {
        return Range.closedOpen(SemanticVersion.min(), SemanticVersion.max());
    }

    /**
     * Returns the sub-ranges of {@code [MIN, MAX)} that are not covered by any of the supplied ranges. An empty
     * result means the supplied ranges comprehensively cover every possible version.
     */
    @Nonnull
    public static Set<Range<SemanticVersion>> uncovered(@Nonnull Collection<Range<SemanticVersion>> ranges) {
        RangeSet<SemanticVersion> rangeSet = TreeRangeSet.create();
        ranges.forEach(rangeSet::add);
        return rangeSet.complement().subRangeSet(all()).asRanges();
    }

    /**
     * Returns the pairwise non-empty intersections of the supplied ranges, i.e. the regions where two or more ranges
     * overlap. An empty result means the supplied ranges are mutually exclusive.
     */
    @Nonnull
    public static Set<Range<SemanticVersion>> overlapping(@Nonnull List<Range<SemanticVersion>> ranges) {
        final Set<Range<SemanticVersion>> overlaps = new HashSet<>();
        for (int i = 0; i < ranges.size(); i++) {
            for (int j = i + 1; j < ranges.size(); j++) {
                final Range<SemanticVersion> a = ranges.get(i);
                final Range<SemanticVersion> b = ranges.get(j);
                if (a.isConnected(b)) {
                    final Range<SemanticVersion> intersection = a.intersection(b);
                    if (!intersection.isEmpty()) {
                        overlaps.add(intersection);
                    }
                }
            }
        }
        return overlaps;
    }
}
