/*
 * PlanHashable.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * A more stable version of {@link Object#hashCode}.
 */
@API(API.Status.UNSTABLE)
public interface PlanHashable {
    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @return a stable hash code
     */
    int planHash();

    static int planHash(@Nonnull Iterable<? extends PlanHashable> hashables) {
        int result = 1;
        for (PlanHashable hashable : hashables) {
            result = 31 * result + (hashable != null ?  hashable.planHash() : 0);
        }
        return result;
    }

    static int planHash(PlanHashable... hashables) {
        return planHash(Arrays.asList(hashables));
    }

    static int objectPlanHash(@Nullable Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Enum) {
            return ((Enum)obj).name().hashCode();
        }
        if (obj instanceof Iterable<?>) {
            return iterablePlanHash((Iterable<?>) obj);
        }
        if (obj instanceof PlanHashable) {
            return ((PlanHashable)obj).planHash();
        }
        return obj.hashCode();
    }

    static int iterablePlanHash(@Nonnull Iterable<?> objects) {
        int result = 1;
        for (Object object : objects) {
            result = 31 * result + objectPlanHash(object);
        }
        return result;
    }

    static int objectsPlanHash(Object... objects) {
        return objectPlanHash(Arrays.asList(objects));
    }
}
