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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PFrameSpecification;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Specification of a window frame including its type, boundaries, and exclusion mode.
 *
 * @param frameType the type of frame (ROW, RANGE, or GROUPS)
 * @param left the left (start) boundary of the frame
 * @param right the right (end) boundary of the frame
 * @param exclusion the exclusion mode for the frame
 */
public record WindowFrameSpecification(@Nonnull FrameType frameType, @Nonnull FrameBoundary left,
                                       @Nonnull FrameBoundary right, @Nonnull Exclusion exclusion) {

    @Nonnull
    public PFrameSpecification toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PFrameSpecification.newBuilder()
                .setFrameType(frameTypeToProto(frameType))
                .setLeft(frameBoundaryToProto(serializationContext, left))
                .setRight(frameBoundaryToProto(serializationContext, right))
                .setExclusion(exclusionToProto(exclusion))
                .build();
    }

    @Nonnull
    public static WindowFrameSpecification fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PFrameSpecification proto) {
        return new WindowFrameSpecification(
                frameTypeFromProto(proto.getFrameType()),
                frameBoundaryFromProto(serializationContext, proto.getLeft()),
                frameBoundaryFromProto(serializationContext, proto.getRight()),
                exclusionFromProto(proto.getExclusion()));
    }

    @Nonnull
    public ExplainTokens explain() {
        final var tokens = new ExplainTokens();
        tokens.addKeyword(frameType.name().toUpperCase(Locale.ROOT))
                .addWhitespace().addKeyword("BETWEEN");
        describeBoundary(tokens, left, true);
        tokens.addWhitespace().addKeyword("AND");
        describeBoundary(tokens, right, false);
        if (exclusion != Exclusion.NO_OTHER) {
            tokens.addWhitespace().addKeyword("EXCLUDE").addWhitespace().addKeyword(explainExclusion(exclusion));
        }
        return tokens;
    }

    public boolean isDefault() {
        return frameType == FrameType.ROW && left == Unbounded.INSTANCE && right == Unbounded.INSTANCE
                && exclusion == Exclusion.NO_OTHER;
    }

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
        } else if (boundary instanceof Bounded bounded) {
            tokens.addWhitespace().addNested(bounded.limit().explain().getExplainTokens()).addWhitespace()
                    .addKeyword(isLeft ? "PRECEDING" : "FOLLOWING");
        } else if (boundary instanceof CurrentRow) {
            tokens.addWhitespace().addKeyword("CURRENT").addWhitespace().addKeyword("ROW");
        }
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
    private static PFrameSpecification.PFrameType frameTypeToProto(@Nonnull final FrameType frameType) {
        return switch (frameType) {
            case ROW -> PFrameSpecification.PFrameType.ROW;
            case RANGE -> PFrameSpecification.PFrameType.RANGE;
            case GROUPS -> PFrameSpecification.PFrameType.GROUPS;
        };
    }

    @Nonnull
    private static FrameType frameTypeFromProto(@Nonnull final PFrameSpecification.PFrameType proto) {
        return switch (proto) {
            case ROW -> FrameType.ROW;
            case RANGE -> FrameType.RANGE;
            case GROUPS -> FrameType.GROUPS;
        };
    }

    @Nonnull
    private static PFrameSpecification.PFrameBoundary frameBoundaryToProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                           @Nonnull final FrameBoundary boundary) {
        if (boundary instanceof Unbounded) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setUnbounded(true).build();
        } else if (boundary instanceof Bounded bounded) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setBoundedLimit(bounded.limit().toValueProto(serializationContext)).build();
        } else if (boundary instanceof CurrentRow) {
            return PFrameSpecification.PFrameBoundary.newBuilder().setCurrentRow(true).build();
        } else {
            throw new IllegalArgumentException("unknown frame boundary type: " + boundary.getClass());
        }
    }

    @Nonnull
    private static FrameBoundary frameBoundaryFromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PFrameSpecification.PFrameBoundary proto) {
        if (proto.hasUnbounded()) {
            return Unbounded.INSTANCE;
        } else if (proto.hasBoundedLimit()) {
            return new Bounded(Value.fromValueProto(serializationContext, proto.getBoundedLimit()));
        } else if (proto.hasCurrentRow()) {
            return new CurrentRow();
        } else {
            throw new IllegalArgumentException("unknown frame boundary proto case");
        }
    }

    @Nonnull
    private static PFrameSpecification.PExclusion exclusionToProto(@Nonnull final Exclusion exclusion) {
        return switch (exclusion) {
            case NO_OTHER -> PFrameSpecification.PExclusion.NO_OTHER;
            case CURRENT_ROW -> PFrameSpecification.PExclusion.CURRENT_ROW;
            case GROUP -> PFrameSpecification.PExclusion.GROUP;
            case TIES -> PFrameSpecification.PExclusion.TIES;
        };
    }

    @Nonnull
    private static Exclusion exclusionFromProto(@Nonnull final PFrameSpecification.PExclusion proto) {
        return switch (proto) {
            case NO_OTHER -> Exclusion.NO_OTHER;
            case CURRENT_ROW -> Exclusion.CURRENT_ROW;
            case GROUP -> Exclusion.GROUP;
            case TIES -> Exclusion.TIES;
        };
    }

    public enum FrameType {
        ROW,
        RANGE,
        GROUPS
    }

    public sealed interface FrameBoundary permits Unbounded, Bounded, CurrentRow {
    }

    public enum Unbounded implements FrameBoundary {
        INSTANCE
    }

    public record Bounded(@Nonnull Value limit) implements FrameBoundary {
        public Bounded {
            if (!limit.isConstant()) {
                throw new IllegalArgumentException("window frame boundary limit must be a constant value");
            }
        }
    }

    public record CurrentRow() implements FrameBoundary {
    }

    public enum Exclusion {
        NO_OTHER,
        CURRENT_ROW,
        GROUP,
        TIES
    }
}
