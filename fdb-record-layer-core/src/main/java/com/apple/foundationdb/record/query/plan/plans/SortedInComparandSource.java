/*
 * SortedInComparandSource.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInSource;
import com.apple.foundationdb.record.planprotos.PSortedInComparandSource;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Variation of {@link InComparandSource} where the values should be returned in a sorted order.
 */
@API(API.Status.INTERNAL)
public class SortedInComparandSource extends InComparandSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_SORTED_IN_COMPARAND_SOURCE = new ObjectPlanHash("Sorted-In-Comparand");

    private final boolean reverse;

    protected SortedInComparandSource(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PSortedInComparandSource sortedInComparandSourceProto) {
        super(serializationContext, Objects.requireNonNull(sortedInComparandSourceProto.getSuper()));
        Verify.verify(sortedInComparandSourceProto.hasReverse());
        this.reverse = sortedInComparandSourceProto.getReverse();
    }

    public SortedInComparandSource(@Nonnull final String bindingName, @Nonnull Comparisons.Comparison comparison, boolean reverse) {
        super(bindingName, comparison);
        this.reverse = reverse;
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, baseHash(mode, OBJECT_PLAN_HASH_SORTED_IN_COMPARAND_SOURCE), getComparison(), reverse);
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    @Override
    protected List<Object> getValues(@Nullable final EvaluationContext context) {
        List<Object> unsortedValues = super.getValues(context);
        return Objects.requireNonNull(InSource.sortValues(unsortedValues, reverse));
    }

    @Nonnull
    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SortedInComparandSource inComparandSource = (SortedInComparandSource)o;
        if (!getBindingName().equals(inComparandSource.getBindingName())) {
            return false;
        }
        return super.equals(inComparandSource) && reverse == inComparandSource.reverse;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison(), reverse);
    }

    @Nonnull
    @Override
    public PSortedInComparandSource toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PSortedInComparandSource.newBuilder()
                .setSuper(toInComparandSourceProto(serializationContext))
                .setReverse(reverse)
                .build();
    }

    @Nonnull
    @Override
    protected PInSource toInSourceProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInSource.newBuilder().setSortedInComparandSource(toProto(serializationContext)).build();
    }

    @Nonnull
    public static SortedInComparandSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PSortedInComparandSource sortedInComparandSourceProto) {
        return new SortedInComparandSource(serializationContext, sortedInComparandSourceProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PSortedInComparandSource, SortedInComparandSource> {
        @Nonnull
        @Override
        public Class<PSortedInComparandSource> getProtoMessageClass() {
            return PSortedInComparandSource.class;
        }

        @Nonnull
        @Override
        public SortedInComparandSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PSortedInComparandSource sortedInComparandSourceProto) {
            return SortedInComparandSource.fromProto(serializationContext, sortedInComparandSourceProto);
        }
    }
}
