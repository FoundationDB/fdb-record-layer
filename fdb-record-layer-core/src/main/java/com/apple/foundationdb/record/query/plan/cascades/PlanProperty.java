/*
 * PlanProperty.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;

import javax.annotation.Nonnull;

/**
 * Base interface to capture attributes for plans.
 * An instance of this usually class serves as a key in maps much like an enum, but provides strong typing.
 * @param <A> the type representing the actual property
 */
public interface PlanProperty<A> {
    /**
     * Method to narrow the type from {@link Object} to the declared type of the attribute. Note that
     * the caller must guarantee that the narrowing is well-defined and successful.
     * @param object an object that actually is of dynamic type {@code A}
     * @return the narrowed object of type {@code A}
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default A narrowAttribute(@Nonnull final Object object) {
        return (A)object;
    }

    @Nonnull
    RecordQueryPlanVisitor<A> createVisitor();
}
