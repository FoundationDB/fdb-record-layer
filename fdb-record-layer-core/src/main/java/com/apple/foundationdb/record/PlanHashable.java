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
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A more stable version of {@link Object#hashCode}.
 * The planHash semantics are different from {@link Object#hashCode} in a few ways:
 * <UL>
 *     <LI>{@link #planHash(PlanHashMode)} values should be stable across runtime instance changes. The reason is that
 *     these values can be used to validate outstanding continuations, and a change in hash value caused by an
 *     application restart or refactoring will invalidate all those outstanding continuations</LI>
 *     <LI>{@link #planHash(PlanHashMode)} supports multiple flavors of hash calculations (See {@link PlanHashMode}).
 *     The various kinds of plan hash algorithms are used for different purposes and include/exclude different parts of
 *     the target query plan</LI>
 *     <LI>{@link #planHash(PlanHashMode)} is meant to imply a certain identity of a plan, and reflects on the entire
 *     structure of the plan. The intent is to be able to correlate various plans for "identity" (using different
 *     definitions for this identity as specified by {@link PlanHashMode}). This requirement drives a desire to reduce
 *     collisions as much as possible since not in all cases can we actually use "equals" to verify identity
 *     (e.g. log messages)</LI>
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
    }

    /**
     * A mode for a plan hash which captures both kind and version. One reason we use an enum here is to force
     * implementors to declare a new version, and to avoid greater than/less than comparisons. In reality
     * (the future will tell), we only want at most two versions to not be deprecated.
     */
    enum PlanHashMode {
        VL0(PlanHashKind.LEGACY, 0),
        VC0(PlanHashKind.FOR_CONTINUATION, 0);

        private final PlanHashKind kind;
        private final int numericVersion;

        PlanHashMode(final PlanHashKind kind, final int numericVersion) {
            this.kind = kind;
            this.numericVersion = numericVersion;
        }

        /**
         * Get the plan hash kind associated with this mode.
         * @return the plan hash kind
         */
        public PlanHashKind getKind() {
            return kind;
        }

        /**
         * Returns a numeric version. Do not use this information if you can. This method should strictly be used
         * when this method is serialized, etc. However, it should not be used to switch on different plan hash
         * versions.
         * @return a numerical version
         */
        public int getNumericVersion() {
            return numericVersion;
        }
    }

    PlanHashMode CURRENT_LEGACY = PlanHashMode.VL0;
    PlanHashMode CURRENT_FOR_CONTINUATION = PlanHashMode.VC0;

    @Nonnull
    private static PlanHashMode currentHashMode(@Nonnull final PlanHashKind kind) {
        switch (kind) {
            case LEGACY:
                return CURRENT_LEGACY;
            case FOR_CONTINUATION:
                return CURRENT_FOR_CONTINUATION;
            default:
                throw new RecordCoreException("unsupported plan hash kind");
        }
    }

    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @param hashMode the "mode" of hash to calculate. Each mode contains a hash kind which has a particular logic with
     * regards to included and excluded items. It is also versioned to support for the evolution of the plan hash when
     * the underlying plan objects evolve
     * @return a stable hash code
     */
    int planHash(@Nonnull PlanHashMode hashMode);

    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @param kind the "kind" of hash to calculate. Each kind of hash has a particular logic with regards to included and excluded items.
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    default int planHash(@Nonnull final PlanHashKind kind) {
        return planHash(currentHashMode(kind));
    }

    /**
     * Parameterless overload of planHash(). This method delegates to the {@link #planHash(PlanHashKind)}. This method
     * is deprecated. In order to achieve the same result, use {@link #planHash(PlanHashKind)} directly passing in
     * {@link PlanHashKind#LEGACY}.
     * @return a stable ash code using {@link PlanHashKind#LEGACY}
     */
    @Deprecated(forRemoval = true)
    default int planHash() {
        return planHash(PlanHashKind.LEGACY);
    }

    /**
     * This method is deprecated. Use {@link #planHash(PlanHashMode, Iterable)} instead.
     * @param kind plan hash kind
     * @param hashables an iterable to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int planHash(@Nonnull final PlanHashKind kind, @Nonnull final Iterable<? extends PlanHashable> hashables) {
        return planHash(currentHashMode(kind), hashables);
    }

    static int planHash(@Nonnull final PlanHashMode mode, @Nonnull final Iterable<? extends PlanHashable> hashables) {
        int result = 1;
        for (PlanHashable hashable : hashables) {
            result = 31 * result + (hashable != null ? hashable.planHash(mode) : 0);
        }
        return result;
    }

    /**
     * This method is deprecated. Use {@link #planHash(PlanHashMode, PlanHashable...)} instead.
     * @param kind plan hash kind
     * @param hashables a varargs array to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int planHash(@Nonnull final PlanHashKind kind, final PlanHashable... hashables) {
        return planHash(currentHashMode(kind), Arrays.asList(hashables));
    }

    static int planHash(@Nonnull final PlanHashMode mode, final PlanHashable... hashables) {
        return planHash(mode, Arrays.asList(hashables));
    }

    /**
     * This method is deprecated. Use {@link #planHashUnordered(PlanHashMode, Iterable)} instead.
     * @param kind plan hash kind
     * @param hashables a varargs array to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int planHashUnordered(@Nonnull final PlanHashKind kind,
                                 @Nonnull final Iterable<? extends PlanHashable> hashables) {
        return planHashUnordered(currentHashMode(kind), hashables);
    }

    static int planHashUnordered(@Nonnull final PlanHashMode hashMode,
                                 @Nonnull final Iterable<? extends PlanHashable> hashables) {
        final ArrayList<Integer> hashes = new ArrayList<>();
        for (PlanHashable hashable : hashables) {
            hashes.add(hashable != null ? hashable.planHash(hashMode) : 0);
        }
        hashes.sort(Comparator.naturalOrder());
        return combineHashes(hashes);
    }

    /**
     * This method is deprecated. Use an order-independent hash algorithm instead, i.e. {@link AbstractSet#hashCode()}.
     * @param strings an iterable of strings
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
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

    /**
     * This method is deprecated. Use {@link #objectPlanHash(PlanHashMode, Object)} instead.
     * @param kind plan hash kind
     * @param obj an object to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int objectPlanHash(@Nonnull final PlanHashKind kind, @Nullable Object obj) {
        return objectPlanHash(currentHashMode(kind), obj);
    }

    static int objectPlanHash(@Nonnull final PlanHashMode mode, @Nullable final Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Enum) {
            return ((Enum)obj).name().hashCode();
        }
        // Unable to change LEGACY behavior
        if (mode.getKind() != PlanHashKind.LEGACY && obj instanceof Set) {
            return setsPlanHash(mode, (Set<?>)obj);
        }
        if (mode.getKind() != PlanHashKind.LEGACY && obj instanceof Map) {
            return mapsPlanHash(mode, (Map<?, ?>)obj);
        }
        if (obj instanceof Iterable<?>) {
            return iterablePlanHash(mode, (Iterable<?>)obj);
        }
        if (obj instanceof Object[]) {
            return objectsPlanHash(mode, (Object[])obj);
        }
        if (obj.getClass().isArray() && obj.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayHash(obj);
        }
        if (obj instanceof PlanHashable) {
            return ((PlanHashable)obj).planHash(mode);
        }
        return obj.hashCode();
    }

    /**
     * This method is deprecated. Use {@link #iterablePlanHash(PlanHashMode, Iterable)} instead.
     * @param kind plan hash kind
     * @param objects an iterable of objects to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int iterablePlanHash(@Nonnull PlanHashKind kind, @Nonnull Iterable<?> objects) {
        return iterablePlanHash(currentHashMode(kind), objects);
    }

    static int iterablePlanHash(@Nonnull PlanHashMode mode, @Nonnull Iterable<?> objects) {
        int result = 1;
        for (Object object : objects) {
            result = 31 * result + objectPlanHash(mode, object);
        }
        return result;
    }

    static int setsPlanHash(@Nonnull final PlanHashMode mode, @Nonnull final Set<?> objects) {
        int result = 1;
        for (Object object : objects) {
            result += 31 * objectPlanHash(mode, object);
        }
        return result;
    }

    static int mapsPlanHash(@Nonnull final PlanHashMode mode, @Nonnull final Map<?, ?> map) {
        int result = 1;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result += 31 * objectsPlanHash(mode, entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * This method is deprecated. Use {@link #objectsPlanHash(PlanHashMode, Object...)} instead.
     * @param kind plan hash kind
     * @param objects a varargs of objects to be hashed
     * @return a stable hash code
     */
    @Deprecated(forRemoval = true)
    static int objectsPlanHash(@Nonnull final PlanHashKind kind, final Object... objects) {
        return objectsPlanHash(currentHashMode(kind), Arrays.asList(objects));
    }

    static int objectsPlanHash(@Nonnull final PlanHashMode mode, final Object... objects) {
        return objectPlanHash(mode, Arrays.asList(objects));
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
