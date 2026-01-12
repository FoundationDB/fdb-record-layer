/*
 * SpatialRestrictions.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;

class SpatialRestrictions {
    private static final Comparator<NodeReferenceWithDistance> COMPARATOR =
            Comparator.nullsFirst(
                    Comparator.comparing(NodeReferenceWithDistance::getDistance)
                            .thenComparing(NodeReference::getPrimaryKey));

    private final int insideLimit;
    private final double minimumRadius;
    @Nullable
    private final Tuple minimumPrimaryKey;

    @Nonnull
    private final NavigableSet<NodeReferenceWithDistance> insideRadius; // inclusive
    @Nonnull
    private final NavigableSet<NodeReferenceWithDistance> outsideRadius; // exclusive

    public SpatialRestrictions(final int insideLimit,
                               final double minimumRadius,
                               @Nullable final Tuple minimumPrimaryKey) {
        this.insideLimit = insideLimit;
        this.minimumRadius = minimumRadius;
        this.minimumPrimaryKey = minimumPrimaryKey;
        this.insideRadius = new TreeSet<>(COMPARATOR);
        this.outsideRadius = new TreeSet<>(COMPARATOR);
    }

    boolean isGreaterThanMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        return compareAgainstMinimum(nodeReferenceWithDistance) < 0;
    }

    boolean isGreaterThanOrEqualMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        return compareAgainstMinimum(nodeReferenceWithDistance) <= 0;
    }

    /**
     * Compare the given {@link NodeReferenceWithDistance} against the
     * {@code (minimumRadius, minimumPrimaryKey)} that was used to initialize the iterator.
     *
     * @param nodeReferenceWithDistance the {@link NodeReferenceWithDistance} against which to compare
     *
     * @return a negative integer, zero, or a positive integer when {@code lastEmitted} is
     * less than, equal, or greater than the parameter {@code t}.
     */
    int compareAgainstMinimum(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        if (minimumRadius == 0.0d || minimumPrimaryKey == null) {
            return -1;
        }

        final int compare = Double.compare(minimumRadius, nodeReferenceWithDistance.getDistance());
        if (compare != 0) {
            return compare;
        }
        return minimumPrimaryKey.compareTo(nodeReferenceWithDistance.getPrimaryKey());
    }

    boolean shouldBeAdded(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        if (insideRadius.size() >= insideLimit) {
            final NodeReferenceWithDistance least = insideRadius.first();
            if (COMPARATOR.compare(nodeReferenceWithDistance, least) <= 0) {
                return false;
            }
        }

        if (isGreaterThanOrEqualMinimum(nodeReferenceWithDistance)) {
            // wrapped greater than minimum
            return !outsideRadius.contains(nodeReferenceWithDistance);
        }

        return !insideRadius.contains(nodeReferenceWithDistance);
    }

    @CanIgnoreReturnValue
    boolean add(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        if (isGreaterThanOrEqualMinimum(nodeReferenceWithDistance)) {
            return outsideRadius.add(nodeReferenceWithDistance);
        }

        if (insideRadius.size() < insideLimit) {
            return insideRadius.add(nodeReferenceWithDistance);
        }

        Verify.verify(insideRadius.size() == insideLimit);

        final NodeReferenceWithDistance least = insideRadius.first();
        if (COMPARATOR.compare(nodeReferenceWithDistance, least) <= 0) {
            return true;
        }

        final boolean exists = insideRadius.add(nodeReferenceWithDistance);
        insideRadius.pollFirst();
        return exists;
    }
}
