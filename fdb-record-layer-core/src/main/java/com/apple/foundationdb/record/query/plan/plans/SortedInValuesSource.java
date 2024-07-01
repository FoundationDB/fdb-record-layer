/*
 * SortedInValuesSource.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInSource;
import com.apple.foundationdb.record.planprotos.PSortedInValuesSource;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * Helper class which represents a specialized {@link InSource} whose input is a list of literal values.
 * The logic in this class enforces the order of the elements in the list by explicitly sorting them at planning time.
 * If reasoning about sorted-ness is not a requirement for a use case, {@link InValuesSource} is preferable.
 * This source is only used by {@link RecordQueryInJoinPlan}s as {@link RecordQueryInUnionPlan} establishes order via
 * an explicit comparison key.
 */
@API(API.Status.INTERNAL)
public class SortedInValuesSource extends InValuesSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_IN_VALUES_SOURCE = new ObjectPlanHash("Sorted-In-Values");

    final boolean isReverse;

    protected SortedInValuesSource(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PSortedInValuesSource sortedInValuesSourceProto) {
        super(serializationContext, Objects.requireNonNull(sortedInValuesSourceProto.getSuper()));
        Verify.verify(sortedInValuesSourceProto.hasReverse());
        this.isReverse = sortedInValuesSourceProto.getReverse();
    }

    public SortedInValuesSource(@Nonnull String bindingName, @Nonnull final List<Object> values, final boolean isReverse) {
        super(bindingName, InSource.sortValues(values, isReverse));
        this.isReverse = isReverse;
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public boolean isReverse() {
        return isReverse;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, baseHash(mode, OBJECT_PLAN_HASH_IN_VALUES_SOURCE), super.planHash(mode), isReverse);
    }

    @Nonnull
    @Override
    public String toString() {
        return getBindingName() + " IN " + getValues() + (isReverse() ? " DESC" : " ASC");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SortedInValuesSource sortedInValuesSource = (SortedInValuesSource)o;
        if (!super.equals(sortedInValuesSource)) {
            return false;
        }
        return isReverse == sortedInValuesSource.isReverse;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isReverse);
    }

    @Nonnull
    @Override
    public PSortedInValuesSource toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PSortedInValuesSource.newBuilder()
                .setSuper(toInValuesSourceProto(serializationContext))
                .setReverse(isReverse)
                .build();
    }

    @Nonnull
    @Override
    protected PInSource toInSourceProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInSource.newBuilder().setSortedInValuesSource(toProto(serializationContext)).build();
    }

    @Nonnull
    public static SortedInValuesSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PSortedInValuesSource sortedInValuesSourceProto) {
        return new SortedInValuesSource(serializationContext, sortedInValuesSourceProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PSortedInValuesSource, SortedInValuesSource> {
        @Nonnull
        @Override
        public Class<PSortedInValuesSource> getProtoMessageClass() {
            return PSortedInValuesSource.class;
        }

        @Nonnull
        @Override
        public SortedInValuesSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PSortedInValuesSource sortedInValuesSourceProto) {
            return SortedInValuesSource.fromProto(serializationContext, sortedInValuesSourceProto);
        }
    }
}
