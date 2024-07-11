/*
 * SortedInParameterSource.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInSource;
import com.apple.foundationdb.record.planprotos.PSortedInParameterSource;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Helper class which represents a specialized {@link InSource} whose input is an outer binding (a parameter).
 * The logic in this class enforces the order of the elements in the list by explicitly sorting them at execution time.
 * If reasoning about sorted-ness is not a requirement for a use case, {@link InParameterSource} is preferable.
 * This source is only used by {@link RecordQueryInJoinPlan}s as {@link RecordQueryInUnionPlan} establishes order via
 * an explicit comparison key.
 */
@API(API.Status.INTERNAL)
public class SortedInParameterSource extends InParameterSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_SORTED_IN_PARAMETER_SOURCE = new ObjectPlanHash("Sorted-In-Parameter");

    private final boolean isReverse;

    protected SortedInParameterSource(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PSortedInParameterSource sortedInParameterSourceProto) {
        super(serializationContext, Objects.requireNonNull(sortedInParameterSourceProto.getSuper()));
        Verify.verify(sortedInParameterSourceProto.hasReverse());
        this.isReverse = sortedInParameterSourceProto.getReverse();
    }

    public SortedInParameterSource(@Nonnull String bindingName, @Nonnull final String parameterName, final boolean isReverse) {
        super(bindingName, parameterName);
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
        return PlanHashable.objectsPlanHash(mode, baseHash(mode, OBJECT_PLAN_HASH_SORTED_IN_PARAMETER_SOURCE), super.planHash(mode), isReverse);
    }

    @Override
    protected int size(@Nonnull final EvaluationContext context) {
        return getBoundValues(context).size();
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    protected List<Object> getValues(@Nullable final EvaluationContext context) {
        final List<Object> values = getBoundValues(Objects.requireNonNull(context));
        // sortValues() guarantees non-null out on non-null in.
        return Objects.requireNonNull(InSource.sortValues(values, isReverse));
    }

    @Nonnull
    @Override
    public String toString() {
        return getBindingName() + " IN $" + getParameterName() + (isReverse() ? " DESC" : " ASC");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final var sortedInParameterSource = (SortedInParameterSource)o;
        if (!super.equals(sortedInParameterSource)) {
            return false;
        }
        return isReverse == sortedInParameterSource.isReverse;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isReverse);
    }

    @Nonnull
    @Override
    public PSortedInParameterSource toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PSortedInParameterSource.newBuilder()
                .setSuper(toInParameterSourceProto(serializationContext))
                .setReverse(isReverse)
                .build();
    }

    @Nonnull
    @Override
    protected PInSource toInSourceProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInSource.newBuilder().setSortedInParameterSource(toProto(serializationContext)).build();
    }

    @Nonnull
    public static SortedInParameterSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PSortedInParameterSource sortedInParameterSourceProto) {
        return new SortedInParameterSource(serializationContext, sortedInParameterSourceProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PSortedInParameterSource, SortedInParameterSource> {
        @Nonnull
        @Override
        public Class<PSortedInParameterSource> getProtoMessageClass() {
            return PSortedInParameterSource.class;
        }

        @Nonnull
        @Override
        public SortedInParameterSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PSortedInParameterSource sortedInParameterSourceProto) {
            return SortedInParameterSource.fromProto(serializationContext, sortedInParameterSourceProto);
        }
    }
}
