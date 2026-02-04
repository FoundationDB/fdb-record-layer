/*
 * CosineDistanceRowNumberValue.java
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
import com.apple.foundationdb.record.planprotos.PCosineDistanceRowNumberValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A windowed value that assigns unique, sequential row numbers to rows ordered by their cosine distance from a reference point.
 * <p>
 * This class implements a window function similar to SQL's {@code ROW_NUMBER()} but specifically for cosine distance
 * calculations. It assigns unique sequential numbers (starting from 1) to rows based on their angular similarity to a
 * reference vector, with more similar vectors (smaller cosine distance) receiving lower numbers. Unlike {@code RANK()},
 * each row receives a unique number even if multiple rows have equal distances.
 * </p>
 * <p>
 * Cosine distance measures the angular difference between vectors, ranging from 0 (identical direction) to 2 (opposite
 * direction). This metric is commonly used for similarity search in vector spaces where magnitude is less important than
 * direction.
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
 * <strong>Example:</strong> For vectors [(1,0), (0.9,0.1), (0.9,0.1), (0,1)] measured from direction (1,0),
 * the row numbers would be [1, 2, 3, 4] as each row gets a unique sequential number. The first vector has
 * the smallest cosine distance (row number 1), the next two have equal distance but receive consecutive numbers
 * (2 and 3), and the last is most different (row number 4). Note that rows 2 and 3 have the same distance but
 * different row numbers.
 * </p>
 *
 * @see WindowedValue
 * @see Value.IndexOnlyValue
 */
@API(API.Status.EXPERIMENTAL)
public class CosineDistanceRowNumberValue extends WindowedValue implements Value.IndexOnlyValue {
    private static final String NAME = "CosineDistanceRowNumber";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    public CosineDistanceRowNumberValue(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PCosineDistanceRowNumberValue cosineDistanceRowNumberValueProto) {
        super(serializationContext, Objects.requireNonNull(cosineDistanceRowNumberValueProto.getSuper()));
    }

    public CosineDistanceRowNumberValue(@Nonnull Iterable<? extends Value> partitioningValues,
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
    public CosineDistanceRowNumberValue withChildren(final Iterable<? extends Value> newChildren) {
        final var childrenPair = splitNewChildren(newChildren);
        return new CosineDistanceRowNumberValue(childrenPair.getKey(), childrenPair.getValue());
    }

    @Nonnull
    @Override
    public PCosineDistanceRowNumberValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PCosineDistanceRowNumberValue.newBuilder().setSuper(toWindowedValueProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCosineDistanceRowNumberValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static CosineDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PCosineDistanceRowNumberValue rowNumberValueProto) {
        return new CosineDistanceRowNumberValue(serializationContext, rowNumberValueProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCosineDistanceRowNumberValue, CosineDistanceRowNumberValue> {
        @Nonnull
        @Override
        public Class<PCosineDistanceRowNumberValue> getProtoMessageClass() {
            return PCosineDistanceRowNumberValue.class;
        }

        @Nonnull
        @Override
        public CosineDistanceRowNumberValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PCosineDistanceRowNumberValue cosineDistanceRowNumberValueProto) {
            return CosineDistanceRowNumberValue.fromProto(serializationContext, cosineDistanceRowNumberValueProto);
        }
    }
}
