/*
 * IndexScanRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.RTree;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link IndexScanBounds} for a multidimensional index scan.
 */
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexScanBounds implements IndexScanBounds {
    @Nonnull
    private final TupleRange prefixRange;

    @Nonnull
    private final SpatialPredicate spatialPredicate;

    public MultidimensionalIndexScanBounds(@Nonnull final TupleRange prefixRange, @Nonnull final SpatialPredicate spatialPredicate) {
        this.prefixRange = prefixRange;
        this.spatialPredicate = spatialPredicate;
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

    public boolean overlapsMbr(@Nonnull RTree.Rectangle mbr) {
        return spatialPredicate.overlapsMbr(mbr);
    }

    public boolean containsPosition(@Nonnull RTree.Point position) {
        return spatialPredicate.containsPosition(position);
    }

    /**
     * Spatial predicate.
     */
    public interface SpatialPredicate {
        SpatialPredicate TAUTOLOGY = new SpatialPredicate() {
            @Override
            public boolean overlapsMbr(@Nonnull final RTree.Rectangle mbr) {
                return true;
            }

            @Override
            public boolean containsPosition(@Nonnull final RTree.Point position) {
                return true;
            }
        };

        boolean overlapsMbr(@Nonnull RTree.Rectangle mbr);

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
        public boolean overlapsMbr(@Nonnull final RTree.Rectangle mbr) {
            return children.stream()
                    .anyMatch(child -> child.overlapsMbr(mbr));
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
        public boolean overlapsMbr(@Nonnull final RTree.Rectangle mbr) {
            return children.stream()
                    .allMatch(child -> child.overlapsMbr(mbr));
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
        public boolean overlapsMbr(@Nonnull final RTree.Rectangle mbr) {
            Preconditions.checkArgument(mbr.getNumDimensions() == dimensionRanges.size());

            for (int d = 0; d < mbr.getNumDimensions(); d++) {
                final Tuple lowTuple = Tuple.from(mbr.getLow(d));
                final Tuple highTuple = Tuple.from(mbr.getHigh(d));

                final TupleRange dimensionRange = dimensionRanges.get(d);

                switch (dimensionRange.getLowEndpoint()) {
                    case TREE_START:
                        break;
                    case RANGE_INCLUSIVE:
                    case RANGE_EXCLUSIVE:
                        final Tuple dimensionLow = Objects.requireNonNull(dimensionRange.getLow());
                        if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                                TupleHelpers.compare(highTuple, dimensionLow) < 0) {
                            return false;
                        }
                        if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                                TupleHelpers.compare(highTuple, dimensionLow) <= 0) {
                            return false;
                        }
                        break;
                    case TREE_END:
                    case CONTINUATION:
                    case PREFIX_STRING:
                    default:
                        throw new RecordCoreException("do not support endpoint " + dimensionRange.getLowEndpoint());
                }

                switch (dimensionRange.getHighEndpoint()) {
                    case TREE_END:
                        break;
                    case RANGE_INCLUSIVE:
                    case RANGE_EXCLUSIVE:
                        final Tuple dimensionHigh = Objects.requireNonNull(dimensionRange.getHigh());
                        if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                                TupleHelpers.compare(lowTuple, dimensionHigh) > 0) {
                            return false;
                        }
                        if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                                TupleHelpers.compare(highTuple, dimensionHigh) >= 0) {
                            return false;
                        }
                        break;
                    case TREE_START:
                    case CONTINUATION:
                    case PREFIX_STRING:
                    default:
                        throw new RecordCoreException("do not support endpoint " + dimensionRange.getHighEndpoint());
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

                switch (dimensionRange.getLowEndpoint()) {
                    case TREE_START:
                        break;
                    case RANGE_INCLUSIVE:
                    case RANGE_EXCLUSIVE:
                        final Tuple dimensionLow = Objects.requireNonNull(dimensionRange.getLow());
                        if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                                TupleHelpers.compare(coordinate, dimensionLow) < 0) {
                            return false;
                        }
                        if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                                TupleHelpers.compare(coordinate, dimensionLow) <= 0) {
                            return false;
                        }
                        break;
                    case TREE_END:
                    case CONTINUATION:
                    case PREFIX_STRING:
                    default:
                        throw new RecordCoreException("do not support endpoint " + dimensionRange.getLowEndpoint());
                }

                switch (dimensionRange.getHighEndpoint()) {
                    case TREE_END:
                        break;
                    case RANGE_INCLUSIVE:
                    case RANGE_EXCLUSIVE:
                        final Tuple dimensionHigh = Objects.requireNonNull(dimensionRange.getHigh());
                        if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                                TupleHelpers.compare(coordinate, dimensionHigh) > 0) {
                            return false;
                        }
                        if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                                TupleHelpers.compare(coordinate, dimensionHigh) >= 0) {
                            return false;
                        }
                        break;
                    case TREE_START:
                    case CONTINUATION:
                    case PREFIX_STRING:
                    default:
                        throw new RecordCoreException("do not support endpoint " + dimensionRange.getHighEndpoint());
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
