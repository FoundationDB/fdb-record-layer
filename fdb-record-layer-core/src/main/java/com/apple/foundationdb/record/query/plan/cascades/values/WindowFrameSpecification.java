/*
 * WindowFrameSpecification.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PWindowFrameSpecification;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;

import javax.annotation.Nonnull;

/**
 * Specification of a window frame including its type, boundaries, and exclusion mode. A frame narrows the set of rows
 * a window function sees within its partition; it corresponds to the {@code ROWS}/{@code RANGE}/{@code GROUPS} clause
 * of an SQL {@code OVER} specification.
 *
 * @param frameType the type of frame (ROW, RANGE, or GROUPS)
 * @param left the left (start) boundary of the frame
 * @param right the right (end) boundary of the frame
 * @param exclusion the exclusion mode for the frame
 */
@API(API.Status.EXPERIMENTAL)
public record WindowFrameSpecification(@Nonnull FrameType frameType, @Nonnull FrameBoundary left,
                                       @Nonnull FrameBoundary right, @Nonnull Exclusion exclusion) {

    @Nonnull
    public PWindowFrameSpecification toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PWindowFrameSpecification.newBuilder()
                .setFrameType(frameTypeToProto(frameType))
                .setLeft(frameBoundaryToProto(serializationContext, left))
                .setRight(frameBoundaryToProto(serializationContext, right))
                .setExclusion(exclusionToProto(exclusion))
                .build();
    }

    @Nonnull
    public static WindowFrameSpecification fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PWindowFrameSpecification proto) {
        return new WindowFrameSpecification(
                frameTypeFromProto(proto.getFrameType()),
                frameBoundaryFromProto(serializationContext, proto.getLeft()),
                frameBoundaryFromProto(serializationContext, proto.getRight()),
                exclusionFromProto(proto.getExclusion()));
    }

    /**
     * Renders this frame specification the way it would be spelled in SQL.
     *
     * @return the explain tokens describing this frame specification
     */
    @Nonnull
    public ExplainTokens explain() {
        final var tokens = new ExplainTokens();
        tokens.addKeyword(explainFrameType(frameType))
                .addWhitespace().addKeyword("BETWEEN");
        describeBoundary(tokens, left, true);
        tokens.addWhitespace().addKeyword("AND");
        describeBoundary(tokens, right, false);
        if (exclusion != Exclusion.NO_OTHER) {
            tokens.addWhitespace().addKeyword("EXCLUDE").addWhitespace().addKeyword(explainExclusion(exclusion));
        }
        return tokens;
    }

    /**
     * Returns whether this specification is the one implied by an {@code OVER} clause without an explicit frame, in
     * which case callers usually omit it when explaining or hashing the owning window function.
     *
     * @return {@code true} if this is the default specification, {@code false} otherwise
     */
    public boolean isDefault() {
        return frameType == FrameType.ROW && left == Unbounded.INSTANCE && right == Unbounded.INSTANCE
                && exclusion == Exclusion.NO_OTHER;
    }

    /**
     * Returns the frame spanning the entire partition, which is what a window function sees when no frame clause is
     * given.
     *
     * @return the default frame specification
     */
    @Nonnull
    public static WindowFrameSpecification defaultSpecification() {
        return new WindowFrameSpecification(FrameType.ROW, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.NO_OTHER);
    }

    private static void describeBoundary(@Nonnull final ExplainTokens tokens,
                                         @Nonnull final FrameBoundary boundary,
                                         final boolean isLeft) {
        if (boundary instanceof Unbounded) {
            tokens.addWhitespace().addKeyword("UNBOUNDED").addWhitespace()
                    .addKeyword(isLeft ? "PRECEDING" : "FOLLOWING");
        } else if (boundary instanceof Bounded) {
            tokens.addWhitespace().addNested(((Bounded)boundary).limit().explain().getExplainTokens()).addWhitespace()
                    .addKeyword(isLeft ? "PRECEDING" : "FOLLOWING");
        } else if (boundary instanceof CurrentRow) {
            tokens.addWhitespace().addKeyword("CURRENT").addWhitespace().addKeyword("ROW");
        } else {
            throw new RecordCoreException("unknown frame boundary type");
        }
    }

    @Nonnull
    private static String explainFrameType(@Nonnull final FrameType frameType) {
        // SQL spells the row-counting frame unit ROWS; the enum constant is singular
        return switch (frameType) {
            case ROW -> "ROWS";
            case RANGE -> "RANGE";
            case GROUPS -> "GROUPS";
        };
    }

    @Nonnull
    private static String explainExclusion(@Nonnull final Exclusion exclusion) {
        return switch (exclusion) {
            case CURRENT_ROW -> "CURRENT ROW";
            case GROUP -> "GROUP";
            case TIES -> "TIES";
            default -> "NO OTHERS";
        };
    }

    @Nonnull
    private static PWindowFrameSpecification.PFrameType frameTypeToProto(@Nonnull final FrameType frameType) {
        return switch (frameType) {
            case ROW -> PWindowFrameSpecification.PFrameType.ROW;
            case RANGE -> PWindowFrameSpecification.PFrameType.RANGE;
            case GROUPS -> PWindowFrameSpecification.PFrameType.GROUPS;
        };
    }

    @Nonnull
    private static FrameType frameTypeFromProto(@Nonnull final PWindowFrameSpecification.PFrameType proto) {
        return switch (proto) {
            case ROW -> FrameType.ROW;
            case RANGE -> FrameType.RANGE;
            case GROUPS -> FrameType.GROUPS;
        };
    }

    @Nonnull
    private static PWindowFrameSpecification.PFrameBoundary frameBoundaryToProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                                @Nonnull final FrameBoundary boundary) {
        if (boundary instanceof Unbounded) {
            return PWindowFrameSpecification.PFrameBoundary.newBuilder().setUnbounded(true).build();
        } else if (boundary instanceof Bounded) {
            return PWindowFrameSpecification.PFrameBoundary.newBuilder()
                    .setBoundedLimit(((Bounded)boundary).limit().toValueProto(serializationContext)).build();
        } else if (boundary instanceof CurrentRow) {
            return PWindowFrameSpecification.PFrameBoundary.newBuilder().setCurrentRow(true).build();
        } else {
            throw new RecordCoreException("unknown frame boundary type");
        }
    }

    @Nonnull
    private static FrameBoundary frameBoundaryFromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PWindowFrameSpecification.PFrameBoundary proto) {
        if (proto.hasUnbounded()) {
            return Unbounded.INSTANCE;
        } else if (proto.hasBoundedLimit()) {
            return new Bounded(Value.fromValueProto(serializationContext, proto.getBoundedLimit()));
        } else if (proto.hasCurrentRow()) {
            return new CurrentRow();
        } else {
            throw new RecordCoreException("unknown frame boundary proto case");
        }
    }

    @Nonnull
    private static PWindowFrameSpecification.PExclusion exclusionToProto(@Nonnull final Exclusion exclusion) {
        return switch (exclusion) {
            case NO_OTHER -> PWindowFrameSpecification.PExclusion.NO_OTHER;
            case CURRENT_ROW -> PWindowFrameSpecification.PExclusion.CURRENT_ROW;
            case GROUP -> PWindowFrameSpecification.PExclusion.GROUP;
            case TIES -> PWindowFrameSpecification.PExclusion.TIES;
        };
    }

    @Nonnull
    private static Exclusion exclusionFromProto(@Nonnull final PWindowFrameSpecification.PExclusion proto) {
        return switch (proto) {
            case NO_OTHER -> Exclusion.NO_OTHER;
            case CURRENT_ROW -> Exclusion.CURRENT_ROW;
            case GROUP -> Exclusion.GROUP;
            case TIES -> Exclusion.TIES;
        };
    }

    /**
     * The unit a frame boundary is measured in.
     */
    public enum FrameType {
        ROW,
        RANGE,
        GROUPS
    }

    /**
     * One end of a window frame.
     */
    public sealed interface FrameBoundary permits Unbounded, Bounded, CurrentRow {
    }

    /**
     * A boundary extending to the start or the end of the partition.
     */
    public enum Unbounded implements FrameBoundary {
        INSTANCE
    }

    /**
     * A boundary a constant number of rows, values, or peer groups away from the current row.
     *
     * @param limit the offset from the current row; must be constant
     */
    public record Bounded(@Nonnull Value limit) implements FrameBoundary {
        public Bounded {
            if (!limit.isConstant()) {
                throw new RecordCoreArgumentException("window frame boundary limit must be a constant value");
            }
        }
    }

    /**
     * A boundary at the current row.
     */
    public record CurrentRow() implements FrameBoundary {
    }

    /**
     * Rows to exclude from the frame once its boundaries have been established.
     */
    public enum Exclusion {
        NO_OTHER,
        CURRENT_ROW,
        GROUP,
        TIES
    }
}
