/*
 * ObjectPlanHash.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * This class implements a planHash for the member classes of the query plan.
 * As it is required to separate the plan hashes of various structures in the plan model, we need a way to tell each
 * member of the model apart. Using this class we can implement a stable (withstand JVM restart and class refactorings)
 * base hash for each object that can be added to the rest of the plan.
 */
public class ObjectPlanHash implements PlanHashable, QueryHashable {
    @Nonnull
    private final Object id;
    private final int hashCode;

    public ObjectPlanHash(@Nonnull final Object id) {
        this.id = id;
        this.hashCode = id.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashMode mode) {
        return hashCode;
    }

    @Override
    public int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind) {
        return hashCode;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectPlanHash)) {
            return false;
        }
        final ObjectPlanHash that = (ObjectPlanHash)o;
        return hashCode == that.hashCode && Objects.equals(id, that.id);
    }

    @Override
    public String toString() {
        return id.toString();
    }
}
