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
 * The planHash semantics are different than {@link Object#hashCode} in a few ways:
 * <UL>
 *     <LI>{@link #planHash()} values should be stable across runtime instance changes. The reason is that these values can be used to validate
 *     outstanding continuations, and a change in hash value caused by an application restart or refactoring will invalidate all those
 *     outstanding continuations</LI>
 *     <LI>{@link #planHash()} supports multiple flavors of hash calculations (See {@link PlanHashKind}). The various kinds of plan hash algorithms
 *     are used for different purposes and include/exclude different parts of the target query plan</LI>
 *     <LI>{@link #planHash()} is meant to imply a certain identity of a plan, and reflects on the entire structure of the plan.
 *     The intent is to be able to correlate various plans for "identity" (using different definitions for this identity as
 *     specified by {@link PlanHashKind}). This requirement drives a desire to reduce collisions as much as possible since
 *     not in all cases can we actually use "equals" to verify identity (e.g. log messages)</LI>
 * </UL>
 */
@API(API.Status.UNSTABLE)
public interface PlanHashable {
    /**
     * The "kinds" of planHash calculations.
     */
    enum PlanHashKind {
        LEGACY,                       // The original plan hash kind. Here for backwards compatibility, will be removed in the future
        FOR_CONTINUATION,             // Continuation validation plan hash kind: include children, literals and markers. Used for continuation validation
        STRUCTURAL_WITHOUT_LITERALS   // The hash used for query matching: skip all literals and markers
    }

    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @param hashKind the "kind" of hash to calculate. Each kind of hash has a particular logic with regards to included and excluded items.
     * @return a stable hash code
     */
    int planHash(@Nonnull final PlanHashKind hashKind);

    default int planHash() {
        return planHash(PlanHashKind.LEGACY);
    }

    static int planHash(@Nonnull final PlanHashKind hashKind, @Nonnull Iterable<? extends PlanHashable> hashables) {
        int result = 1;
        for (PlanHashable hashable : hashables) {
            result = 31 * result + (hashable != null ? hashable.planHash(hashKind) : 0);
        }
        return result;
    }

    static int planHash(@Nonnull PlanHashKind hashKind, PlanHashable... hashables) {
        return planHash(hashKind, Arrays.asList(hashables));
    }

    static int planHashUnordered(@Nonnull final PlanHashKind hashKind, @Nonnull Iterable<? extends PlanHashable> hashables) {
        final ArrayList<Integer> hashes = new ArrayList<>();
        for (PlanHashable hashable : hashables) {
            hashes.add(hashable != null ? hashable.planHash(hashKind) : 0);
        }
        hashes.sort(Comparator.naturalOrder());
        return combineHashes(hashes);
    }

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

    static int objectPlanHash(@Nonnull final PlanHashKind hashKind, @Nullable Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Enum) {
            return ((Enum)obj).name().hashCode();
        }
        if (obj instanceof Iterable<?>) {
            return iterablePlanHash(hashKind, (Iterable<?>)obj);
        }
        if (obj instanceof Object[]) {
            return objectsPlanHash(hashKind, (Object[])obj);
        }
        if (obj.getClass().isArray() && obj.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayHash(obj);
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

    static int primitiveArrayHash(Object primitiveArray) {
        Class<?> componentType = primitiveArray.getClass().getComponentType();
        if (boolean.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((boolean[])primitiveArray);
        } else if (byte.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((byte[])primitiveArray);
        } else if (char.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((char[])primitiveArray);
        } else if (double.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((double[])primitiveArray);
        } else if (float.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((float[])primitiveArray);
        } else if (int.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((int[])primitiveArray);
        } else if (long.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((long[])primitiveArray);
        } else if (short.class.isAssignableFrom(componentType)) {
            return Arrays.hashCode((short[])primitiveArray);
        } else {
            throw new IllegalArgumentException("Unknown type for hash code: " + componentType);
        }
    }
}
