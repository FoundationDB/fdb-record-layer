/*
 * DotProductDistanceRowNumberValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PDotProductDistanceRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A windowed value that assigns unique, sequential row numbers to rows ordered by their dot product distance from a reference point.
 * <p>
 * This class implements a window function similar to SQL's {@code ROW_NUMBER()} but specifically for dot product distance
 * calculations. It assigns unique sequential numbers (starting from 1) to rows based on their negative dot product from a reference
 * vector, with vectors having larger dot products (more similar) receiving lower numbers. Unlike {@code RANK()}, each row receives
 * a unique number even if multiple rows have equal distances.
 * </p>
 * <p>
 * The dot product distance is defined as the negative dot product, which means that vectors with higher dot products (more aligned)
 * will have smaller distances and receive lower row numbers. This makes it suitable for similarity searches where higher dot products
 * indicate greater similarity.
 * </p>
 * <p>
 * The numbering can optionally be partitioned by one or more expressions, similar to the {@code PARTITION BY} clause
 * in SQL window functions. Within each partition, row numbers are computed independently starting from 1.
 * </p>
 * <p>
 * This value is an {@link Value.IndexOnlyValue}, meaning it can only be computed from pre-calculated values stored
 * in an index and cannot be evaluated from base records or computed on-the-fly.
 * </p>
 * <p>
 * <strong>Example:</strong> For vectors [(1,0), (0,1), (0.7,0.7), (0,0)] measured from (1,0),
 * the dot products would be [1, 0, 0.7, 0], the distances would be [-1, 0, -0.7, 0], and the row numbers would be
 * [1, 2, 3, 4] where the first vector (most aligned) gets row number 1.
 * </p>
 *
 * @see WindowedValue
 * @see Value.IndexOnlyValue
 */
@API(API.Status.EXPERIMENTAL)
public class DotProductDistanceRowNumberValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "DotProductDistanceRowNumber";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public DotProductDistanceRowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PDotProductDistanceRowNumberValue dotProductDistanceRowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(dotProductDistanceRowNumberValueProto.getSuper()));
    }

    public DotProductDistanceRowNumberValue(@Nonnull Iterable<? extends Value> partitioningValues,
                                            @Nonnull Iterable<? extends Value> argumentValues) {
        super(partitioningValues, argumentValues);
    }

    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return basePlanHash(mode, BASE_HASH);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG);
    }

    @Nonnull
    @Override
    public DotProductDistanceRowNumberValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new DotProductDistanceRowNumberValue(childrenPair.getKey(), childrenPair.getValue());
    }

    @Nonnull
    @Override
    public PDotProductDistanceRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PDotProductDistanceRowNumberValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setDotProductDistanceRowNumberValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static DotProductDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                             @Nonnull final PDotProductDistanceRowNumberValue rankValueProto) {
        return new DotProductDistanceRowNumberValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PDotProductDistanceRowNumberValue, DotProductDistanceRowNumberValue> {
        @Nonnull
        @Override
        public Class<PDotProductDistanceRowNumberValue> getProtoMessageClass() {
            return PDotProductDistanceRowNumberValue.class;
        }

        @Nonnull
        @Override
        public DotProductDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                          @Nonnull final PDotProductDistanceRowNumberValue dotProductDistanceRowNumberValueProto) {
            return DotProductDistanceRowNumberValue.fromProto(serializationContext, dotProductDistanceRowNumberValueProto);
        }
    }
}
