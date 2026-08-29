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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.planprotos.PPlanReference;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerializationRegistry;
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.jspecify.annotations.Nullable;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record;

/**
 * Context class defining the state of serialization/deserialization currently in-flight. An object of this class is
 * stateful and mutable and not to be considered thread-safe.
 */
@API(API.Status.INTERNAL)
public class PlanSerializationContext {
    private static final RecordTypeWithNameEquivalence recordTypeWithNameEquivalence = new RecordTypeWithNameEquivalence();

    private final PlanSerializationRegistry registry;
    private final PlanHashMode mode;

    private final IdentityBiMap<RecordQueryPlan, Integer> knownPlansMap;

    private final BiMap<Equivalence.Wrapper<Record>, Integer> knownRecordTypesMap;

    public PlanSerializationContext(final PlanSerializationRegistry registry,
                                    final PlanHashMode mode) {
        this(registry, mode, IdentityBiMap.create(), HashBiMap.create());
    }

    public PlanSerializationContext(final PlanSerializationRegistry registry,
                                    final PlanHashMode mode,
                                    final IdentityBiMap<RecordQueryPlan, Integer> knownPlansMap,
                                    final BiMap<Equivalence.Wrapper<Record>, Integer> knownRecordTypesMap) {
        this.registry = registry;
        this.mode = mode;
        this.knownPlansMap = knownPlansMap;
        this.knownRecordTypesMap = knownRecordTypesMap;
    }

    public PlanSerializationRegistry getRegistry() {
        return registry;
    }

    public PlanHashMode getMode() {
        return mode;
    }

    public static PlanSerializationContext newForCurrentMode() {
        return newForCurrentMode(DefaultPlanSerializationRegistry.INSTANCE);
    }

    public static PlanSerializationContext newForCurrentMode(final PlanSerializationRegistry registry) {
        return new PlanSerializationContext(registry, PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    public PPlanReference toPlanReferenceProto(final RecordQueryPlan recordQueryPlan) {
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

    public RecordQueryPlan fromPlanReferenceProto(final PPlanReference planReferenceProto) {
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

    public int registerReferenceIdForRecordType(final Record recordType) {
        // use a new reference id
        return registerReferenceIdForRecordType(recordType, knownRecordTypesMap.size());
    }

    public int registerReferenceIdForRecordType(final Record recordType, final int referenceId) {
        //
        // First time the type is being visited, set the reference and the message.
        //
        knownRecordTypesMap.put(recordTypeWithNameEquivalence.wrap(recordType), referenceId);
        return referenceId;
    }

    @Nullable
    public Integer lookupReferenceIdForRecordType(final Record type) {
        return knownRecordTypesMap.get(recordTypeWithNameEquivalence.wrap(type));
    }

    @Nullable
    public Record lookupRecordTypeForReferenceId(final int referenceId) {
        final BiMap<Integer, Equivalence.Wrapper<Record>> inverse = knownRecordTypesMap.inverse();
        final Equivalence.Wrapper<Record> wrapper = inverse.get(referenceId);
        return wrapper == null ? null : wrapper.get();
    }

    /**
     * Equivalence that is established on record types including their names and structure.
     */
    @SpotBugsSuppressWarnings(
            value = "HE_INHERITS_EQUALS_USE_HASHCODE",
            justification = "Superclass overloads equals just for documentation. This is also a singleton anyway with a " +
                    "private constructor, so equals and hash code are not useful")
    private static class RecordTypeWithNameEquivalence extends Equivalence<Record> {
        private RecordTypeWithNameEquivalence() {
        }

        @Override
        protected boolean doEquivalent(final Record a, final Record b) {
            if (!a.equals(b)) {
                return false;
            }
            return Objects.equals(a.getName(), b.getName());
        }

        @Override
        protected int doHash(final Record record) {
            return Objects.hash(record.getName(), record.hashCode());
        }
    }
}
