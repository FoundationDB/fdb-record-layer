/*
 * EuclideanSquareDistanceRowNumberValue.java
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
import com.apple.foundationdb.record.planprotos.PEuclideanSquareDistanceRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A windowed value that assigns unique, sequential row numbers to rows ordered by their Euclidean square distance from a reference point.
 * <p>
 * This class implements a window function similar to SQL's {@code ROW_NUMBER()} but specifically for Euclidean square distance
 * calculations. It assigns unique sequential numbers (starting from 1) to rows based on their squared distance from a reference
 * vector, with closer vectors receiving lower numbers. Unlike {@code RANK()}, each row receives a unique number even if
 * multiple rows have equal distances.
 * </p>
 * <p>
 * The Euclidean square distance is the squared Euclidean distance (without taking the square root), which is computationally
 * more efficient while preserving the same ordering as Euclidean distance. This makes it suitable for nearest-neighbor searches
 * where only the relative ordering matters.
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
 * <strong>Example:</strong> For vectors [(1,0), (2,0), (2,0), (5,0)] measured from origin (0,0),
 * the squared distances would be [1, 4, 4, 25], and the row numbers would be [1, 2, 3, 4] as each row gets a unique
 * sequential number. The first vector is closest (row number 1), the next two have equal squared distance but receive
 * consecutive numbers (2 and 3), and the last is furthest (row number 4). Note that rows 2 and 3 have the same distance
 * but different row numbers.
 * </p>
 *
 * @see WindowedValue
 * @see Value.IndexOnlyValue
 */
@API(API.Status.EXPERIMENTAL)
public class EuclideanSquareDistanceRowNumberValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "EuclideanSquareDistanceRowNumber";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public EuclideanSquareDistanceRowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PEuclideanSquareDistanceRowNumberValue euclideanSquareDistanceRowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(euclideanSquareDistanceRowNumberValueProto.getSuper()));
    }

    public EuclideanSquareDistanceRowNumberValue(@Nonnull Iterable<? extends Value> partitioningValues,
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
    public EuclideanSquareDistanceRowNumberValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new EuclideanSquareDistanceRowNumberValue(childrenPair.getKey(), childrenPair.getValue());
    }

    @Nonnull
    @Override
    public PEuclideanSquareDistanceRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PEuclideanSquareDistanceRowNumberValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setEuclideanSquareDistanceRowNumberValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static EuclideanSquareDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                   @Nonnull final PEuclideanSquareDistanceRowNumberValue rankValueProto) {
        return new EuclideanSquareDistanceRowNumberValue(serializationContext, rankValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PEuclideanSquareDistanceRowNumberValue, EuclideanSquareDistanceRowNumberValue> {
        @Nonnull
        @Override
        public Class<PEuclideanSquareDistanceRowNumberValue> getProtoMessageClass() {
            return PEuclideanSquareDistanceRowNumberValue.class;
        }

        @Nonnull
        @Override
        public EuclideanSquareDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                @Nonnull final PEuclideanSquareDistanceRowNumberValue euclideanSquareDistanceRowNumberValueProto) {
            return EuclideanSquareDistanceRowNumberValue.fromProto(serializationContext, euclideanSquareDistanceRowNumberValueProto);
        }
    }
}
