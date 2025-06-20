/*
 * RecordFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;

/**
 * A function to be applied to a record as part of query execution.
 * @param <T> the result type of the function
 */
@API(API.Status.UNSTABLE)
public abstract class RecordFunction<T> implements PlanHashable {

    @Nonnull
    private final String name;

    protected RecordFunction(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract")
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecordFunction<?> that = (RecordFunction) o;

        return this.name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)} so
     * that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashMode mode, ObjectPlanHash baseHash, Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
                return name.hashCode();
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, baseHash, name, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
