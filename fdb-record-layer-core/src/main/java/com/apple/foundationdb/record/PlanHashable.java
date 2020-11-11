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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A more stable version of {@link Object#hashCode}.
 */
@API(API.Status.UNSTABLE)
public interface PlanHashable {
    enum PlanHashKind {
        CONTINUATION,                 // The original plan hash kind: include children, literals and markers. Used for continuation validation
        STRUCTURAL_WITHOUT_LITERALS   // The hash used for query matching: skip all literals and markers
    }

    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @param hashKind the "kind" of hash to calculate. Each kind of hash has a particular logic with regards to included and excluded items.
     * @return a stable hash code
     */
    int planHash(@Nonnull PlanHashKind hashKind);

    default int planHash() {
        return planHash(PlanHashKind.CONTINUATION);
    }

    static int planHash(@Nonnull PlanHashKind hashKind, @Nonnull Iterable<? extends PlanHashable> hashables) {
        int result = 1;
        for (PlanHashable hashable : hashables) {
            result = 31 * result + (hashable != null ? hashable.planHash(hashKind) : 0);
        }
        return result;
    }

    static int planHash(@Nonnull PlanHashKind hashKind, PlanHashable... hashables) {
        return planHash(hashKind, Arrays.asList(hashables));
    }

    static int planHashUnordered(@Nonnull PlanHashKind hashKind, @Nonnull Iterable<? extends PlanHashable> hashables) {
        final ArrayList<Integer> hashes = new ArrayList<>();
        for (PlanHashable hashable : hashables) {
            hashes.add(hashable != null ? hashable.planHash(hashKind) : 0);
        }
        hashes.sort(Comparator.naturalOrder());
        return combineHashes(hashes);
    }

    // TODO: Do we need to add PlanHashKind here too?
    static int stringHashUnordered(@Nonnull Iterable<String> strings) {
        final ArrayList<Integer> hashes = new ArrayList<>();
        for (String str : strings) {
            hashes.add(str != null ? str.hashCode() : 0);
        }
        hashes.sort(Comparator.naturalOrder());
        return combineHashes(hashes);
    }

    static int combineHashes(@Nonnull List<Integer> hashes) {
        int result = 1;
        for (Integer hash : hashes) {
            result = 31 * result + hash;
        }
        return result;
    }

    static int objectPlanHash(@Nonnull PlanHashKind hashKind, @Nullable Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Enum) {
            return ((Enum)obj).name().hashCode();
        }
        if (obj instanceof Iterable<?>) {
            return iterablePlanHash(hashKind, (Iterable<?>)obj);
        }
        if (obj instanceof PlanHashable) {
            return ((PlanHashable)obj).planHash(hashKind);
        }
        return obj.hashCode();
    }

    static int iterablePlanHash(@Nonnull PlanHashKind hashKind, @Nonnull Iterable<?> objects) {
        int result = 1;
        for (Object object : objects) {
            result = 31 * result + objectPlanHash(hashKind, object);
        }
        return result;
    }

    static int objectsPlanHash(@Nonnull PlanHashKind hashKind, Object... objects) {
        return objectPlanHash(hashKind, Arrays.asList(objects));
    }
}
