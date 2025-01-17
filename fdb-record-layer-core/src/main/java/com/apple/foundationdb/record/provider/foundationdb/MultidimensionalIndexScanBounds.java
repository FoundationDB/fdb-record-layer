/*
 * MultidimensionalIndexScanBounds.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link IndexScanBounds} for a multidimensional index scan. A multidimensional scan bounds object contains a
 * {@link #prefixRange} and a {@link SpatialPredicate} which can be almost arbitrarily complex. The prefix range
 * is a regular tuple range informing the index maintainer how to constrain the search over the non-multidimensional
 * fields that can be viewed as a prefix whose data is stored in a regular one-dimensional index. The spatial predicate
 * implements methods to quickly establish geometric overlap and containment. Spatial predicates do have internal
 * structure as they can delegate to contained other spatial predicates thus allowing to form e.g. logical conjuncts
 * or disjuncts.
 */
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexScanBounds implements IndexScanBounds {
    @Nonnull
    private final TupleRange prefixRange;

    @Nonnull
    private final SpatialPredicate spatialPredicate;

    @Nonnull
    private final TupleRange suffixRange;

    public MultidimensionalIndexScanBounds(@Nonnull final TupleRange prefixRange,
                                           @Nonnull final SpatialPredicate spatialPredicate,
                                           @Nonnull final TupleRange suffixRange) {
        this.prefixRange = prefixRange;
        this.spatialPredicate = spatialPredicate;
        this.suffixRange = suffixRange;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_VALUE;
    }

    @Nonnull
    public TupleRange getPrefixRange() {
        return prefixRange;
    }

    @Nonnull
    public SpatialPredicate getSpatialPredicate() {
        return spatialPredicate;
    }

    @Nonnull
    public TupleRange getSuffixRange() {
        return suffixRange;
    }

    /**
     * Method to compute if the rectangle handed in overlaps with this scan bounds object. This method is invoked when
     * the R-tree data structure of a multidimensional index is searched. Note that this method can be implemented using
     * a best-effort approach as it is permissible to indicate overlap between {@code mbr} and {@code this} when there
     * is in fact no overlap. The rate of false-positives directly influences the search performance in the
     * multidimensional index.
     * @param mbr the minimum-bounding {@link RTree.Rectangle}
     * @return {@code true} if {@code this} overlaps with {@code mbr}
     */
    public boolean overlapsMbrApproximately(@Nonnull RTree.Rectangle mbr) {
        return spatialPredicate.overlapsMbrApproximately(mbr);
    }

    /**
     * Method to compute if the point handed in is contained by this scan bounds object.
     * @param position the {@link RTree.Point}
     * @return {@code true} if {@code position} is contained by {@code this}
     */
    public boolean containsPosition(@Nonnull RTree.Point position) {
        return spatialPredicate.containsPosition(position);
    }

    /**
     * Spatial predicate. The implementing classes form a boolean algebra of sorts. Most notably {@link Hypercube}
     * represents the logical variables, while {@link And} and {@link Or} can be used to build up more complex powerful
     * bounds.
     */
    public interface SpatialPredicate {
        SpatialPredicate TAUTOLOGY = new SpatialPredicate() {
            @Override
            public boolean overlapsMbrApproximately(@Nonnull final RTree.Rectangle mbr) {
                return true;
            }

            @Override
            public boolean containsPosition(@Nonnull final RTree.Point position) {
                return true;
            }
        };

        boolean overlapsMbrApproximately(@Nonnull RTree.Rectangle mbr);

        boolean containsPosition(@Nonnull RTree.Point position);
    }

    /**
     * Scan bounds that consists of other {@link SpatialPredicate}s to form a logical OR.
     */
    public static class Or implements SpatialPredicate {
        @Nonnull
        private final List<SpatialPredicate> children;

        public Or(@Nonnull final List<SpatialPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        @Override
        public boolean overlapsMbrApproximately(@Nonnull final RTree.Rectangle mbr) {
            return children.stream()
                    .anyMatch(child -> child.overlapsMbrApproximately(mbr));
        }

        @Override
        public boolean containsPosition(@Nonnull final RTree.Point position) {
            return children.stream()
                    .anyMatch(child -> child.containsPosition(position));
        }

        @Override
        public String toString() {
            return children.stream().map(Object::toString).collect(Collectors.joining(" or "));
        }
    }

    /**
     * Scan bounds that consists of other {@link SpatialPredicate}s to form a logical AND.
     */
    public static class And implements SpatialPredicate {
        @Nonnull
        private final List<SpatialPredicate> children;

        public And(@Nonnull final List<SpatialPredicate> children) {
            this.children = ImmutableList.copyOf(children);
        }

        @Override
        public boolean overlapsMbrApproximately(@Nonnull final RTree.Rectangle mbr) {
            return children.stream()
                    .allMatch(child -> child.overlapsMbrApproximately(mbr));
        }

        @Override
        public boolean containsPosition(@Nonnull final RTree.Point position) {
            return children.stream()
                    .allMatch(child -> child.containsPosition(position));
        }

        @Override
        public String toString() {
            return children.stream().map(Object::toString).collect(Collectors.joining(" and "));
        }
    }

    /**
     * Scan bounds describing an n-dimensional hypercube.
     */
    public static class Hypercube implements SpatialPredicate {
        @Nonnull
        private final List<TupleRange> dimensionRanges;

        public Hypercube(@Nonnull final List<TupleRange> dimensionRanges) {
            this.dimensionRanges = ImmutableList.copyOf(dimensionRanges);
        }

        @Override
        public boolean overlapsMbrApproximately(@Nonnull final RTree.Rectangle mbr) {
            Preconditions.checkArgument(mbr.getNumDimensions() == dimensionRanges.size());

            for (int d = 0; d < mbr.getNumDimensions(); d++) {
                final Tuple lowTuple = Tuple.from(mbr.getLow(d));
                final Tuple highTuple = Tuple.from(mbr.getHigh(d));
                final TupleRange dimensionRange = dimensionRanges.get(d);
                if (!dimensionRange.overlaps(lowTuple, highTuple)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean containsPosition(@Nonnull final RTree.Point position) {
            Preconditions.checkArgument(position.getNumDimensions() == dimensionRanges.size());

            for (int d = 0; d < position.getNumDimensions(); d++) {
                final Tuple coordinate = Tuple.from(position.getCoordinate(d));
                final TupleRange dimensionRange = dimensionRanges.get(d);
                if (!dimensionRange.contains(coordinate)) {
                    return false;
                }
            }
            return true;
        }

        @Nonnull
        public List<TupleRange> getDimensionRanges() {
            return dimensionRanges;
        }

        @Override
        public String toString() {
            return "HyperCube:[" + dimensionRanges + "]";
        }
    }
}
