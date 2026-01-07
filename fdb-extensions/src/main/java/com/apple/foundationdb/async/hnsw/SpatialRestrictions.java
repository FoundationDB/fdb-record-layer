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
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;

class SpatialRestrictions {
    private static final Comparator<Equivalence.Wrapper<NodeReferenceWithDistance>> COMPARATOR =
            Comparator.comparing(Equivalence.Wrapper::get,
                    Comparator.nullsFirst(
                            Comparator.comparing(NodeReferenceWithDistance::getDistance)
                                    .thenComparing(NodeReference::getPrimaryKey)));

    private static final PrimaryKeyEquivalence PRIMARY_KEY_EQUIVALENCE = new PrimaryKeyEquivalence();

    private final int insideLimit;
    private final double minimumRadius;
    @Nullable
    private final Tuple minimumPrimaryKey;

    @Nonnull
    private final NavigableSet<Equivalence.Wrapper<NodeReferenceWithDistance>> insideRadius; // inclusive
    @Nonnull
    private final NavigableSet<Equivalence.Wrapper<NodeReferenceWithDistance>> outsideRadius; // exclusive

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
        final Equivalence.Wrapper<NodeReferenceWithDistance> wrapped =
                PRIMARY_KEY_EQUIVALENCE.wrap(nodeReferenceWithDistance);
        if (insideRadius.size() >= insideLimit) {
            final Equivalence.Wrapper<NodeReferenceWithDistance> least = insideRadius.first();
            if (COMPARATOR.compare(wrapped, least) <= 0) {
                return false;
            }
        }

        if (isGreaterThanOrEqualMinimum(wrapped.get())) {
            // wrapped greater than minimum
            return !outsideRadius.contains(wrapped);
        }

        return !insideRadius.contains(wrapped);
    }

    @CanIgnoreReturnValue
    boolean add(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
        final Equivalence.Wrapper<NodeReferenceWithDistance> wrapped =
                PRIMARY_KEY_EQUIVALENCE.wrap(nodeReferenceWithDistance);

        if (isGreaterThanOrEqualMinimum(wrapped.get())) {
            return outsideRadius.add(wrapped);
        }

        if (insideRadius.size() < insideLimit) {
            return insideRadius.add(wrapped);
        }

        Verify.verify(insideRadius.size() == insideLimit);

        final Equivalence.Wrapper<NodeReferenceWithDistance> least = insideRadius.first();
        if (COMPARATOR.compare(wrapped, least) <= 0) {
            return true;
        }

        final boolean exists = insideRadius.add(wrapped);
        insideRadius.pollFirst();
        return exists;
    }

    private static class PrimaryKeyEquivalence extends Equivalence<NodeReferenceWithDistance> {
        @Override
        protected boolean doEquivalent(@Nonnull final NodeReferenceWithDistance a,
                                       @Nonnull final NodeReferenceWithDistance b) {
            return a.getPrimaryKey().equals(b.getPrimaryKey());
        }

        @Override
        protected int doHash(@Nonnull final NodeReferenceWithDistance nodeReferenceWithDistance) {
            return nodeReferenceWithDistance.hashCode();
        }
    }
}
