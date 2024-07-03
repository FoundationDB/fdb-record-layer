/*
 * PlanSerializationContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.planprotos.PPlanReference;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerializationRegistry;
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Context class defining the state of serialization/deserialization currently in-flight. An object of this class is
 * stateful and mutable and not to be considered thread-safe.
 */
@API(API.Status.INTERNAL)
public class PlanSerializationContext {
    @Nonnull
    private static final RecordTypeWithNameEquivalence recordTypeWithNameEquivalence = new RecordTypeWithNameEquivalence();

    @Nonnull
    private final PlanSerializationRegistry registry;
    @Nonnull
    private final PlanHashMode mode;

    @Nonnull
    private final IdentityBiMap<RecordQueryPlan, Integer> knownPlansMap;

    @Nonnull
    private final BiMap<Equivalence.Wrapper<Type.Record>, Integer> knownRecordTypesMap;

    public PlanSerializationContext(@Nonnull final PlanSerializationRegistry registry,
                                    @Nonnull final PlanHashMode mode) {
        this(registry, mode, IdentityBiMap.create(), HashBiMap.create());
    }

    public PlanSerializationContext(@Nonnull final PlanSerializationRegistry registry,
                                    @Nonnull final PlanHashMode mode,
                                    @Nonnull final IdentityBiMap<RecordQueryPlan, Integer> knownPlansMap,
                                    @Nonnull final BiMap<Equivalence.Wrapper<Type.Record>, Integer> knownRecordTypesMap) {
        this.registry = registry;
        this.mode = mode;
        this.knownPlansMap = knownPlansMap;
        this.knownRecordTypesMap = knownRecordTypesMap;
    }

    @Nonnull
    public PlanSerializationRegistry getRegistry() {
        return registry;
    }

    @Nonnull
    public PlanHashMode getMode() {
        return mode;
    }

    @Nonnull
    public static PlanSerializationContext newForCurrentMode() {
        return newForCurrentMode(DefaultPlanSerializationRegistry.INSTANCE);
    }

    @Nonnull
    public static PlanSerializationContext newForCurrentMode(@Nonnull final PlanSerializationRegistry registry) {
        return new PlanSerializationContext(registry, PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Nonnull
    public PPlanReference toPlanReferenceProto(@Nonnull final RecordQueryPlan recordQueryPlan) {
        Integer referenceId = knownPlansMap.getUnwrapped(recordQueryPlan);
        if (referenceId != null) {
            //
            // Plan has already been visited -- just set the reference.
            //
            return PPlanReference.newBuilder().setReferenceId(referenceId).build();
        }

        //
        // First time the plan is being visited, set the reference and the message.
        //
        referenceId = knownPlansMap.size();
        knownPlansMap.putUnwrapped(recordQueryPlan, referenceId);
        return PPlanReference.newBuilder()
                .setReferenceId(referenceId)
                .setRecordQueryPlan(recordQueryPlan.toRecordQueryPlanProto(this))
                .build();
    }

    @Nonnull
    public RecordQueryPlan fromPlanReferenceProto(@Nonnull final PPlanReference planReferenceProto) {
        final IdentityBiMap<Integer, RecordQueryPlan> inverse = knownPlansMap.inverse();
        Verify.verify(planReferenceProto.hasReferenceId());
        final int referenceId = planReferenceProto.getReferenceId();
        if (inverse.containsKeyUnwrapped(referenceId)) {
            return Objects.requireNonNull(inverse.getUnwrapped(referenceId));
        }
        final RecordQueryPlan recordQueryPlan = RecordQueryPlan.fromRecordQueryPlanProto(this, Objects.requireNonNull(planReferenceProto.getRecordQueryPlan()));
        Verify.verify(knownPlansMap.putUnwrapped(recordQueryPlan, referenceId) == null);
        return recordQueryPlan;
    }

    public int registerReferenceIdForRecordType(@Nonnull final Type.Record recordType) {
        // use a new reference id
        return registerReferenceIdForRecordType(recordType, knownRecordTypesMap.size());
    }

    public int registerReferenceIdForRecordType(@Nonnull final Type.Record recordType, final int referenceId) {
        //
        // First time the type is being visited, set the reference and the message.
        //
        knownRecordTypesMap.put(recordTypeWithNameEquivalence.wrap(recordType), referenceId);
        return referenceId;
    }

    @Nullable
    public Integer lookupReferenceIdForRecordType(@Nonnull final Type.Record type) {
        return knownRecordTypesMap.get(recordTypeWithNameEquivalence.wrap(type));
    }

    @Nullable
    public Type.Record lookupRecordTypeForReferenceId(final int referenceId) {
        final BiMap<Integer, Equivalence.Wrapper<Type.Record>> inverse = knownRecordTypesMap.inverse();
        final Equivalence.Wrapper<Type.Record> wrapper = inverse.get(referenceId);
        return wrapper == null ? null : wrapper.get();
    }

    /**
     * Equivalence that is established on record types including their names and structure.
     */
    private static class RecordTypeWithNameEquivalence extends Equivalence<Type.Record> {
        @Override
        protected boolean doEquivalent(@Nonnull final Type.Record a, @Nonnull final Type.Record b) {
            if (!a.equals(b)) {
                return false;
            }
            return Objects.equals(a.getName(), b.getName());
        }

        @Override
        protected int doHash(@Nonnull final Type.Record record) {
            return Objects.hash(record.getName(), record.hashCode());
        }
    }
}
