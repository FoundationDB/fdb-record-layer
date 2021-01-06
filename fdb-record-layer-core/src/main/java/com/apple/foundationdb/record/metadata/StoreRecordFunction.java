/*
 * StoreRecordFunction.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;

import javax.annotation.Nonnull;

/**
 * Record function that should be evaluated for some record against
 * an {@link FDBRecordStoreBase} instance.
 * @param <T> the type of the result value
 */
@API(API.Status.MAINTAINED)
public class StoreRecordFunction<T> extends RecordFunction<T> {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Store-Record-Function");

    public StoreRecordFunction(@Nonnull String name) {
        super(name);
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashKind hashKind) {
        return super.basePlanHash(hashKind, BASE_HASH);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return super.baseQueryHash(hashKind, BASE_HASH);
    }
}
