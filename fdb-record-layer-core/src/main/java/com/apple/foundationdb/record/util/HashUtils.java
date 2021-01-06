/*
 * HashUtils.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.QueryHashable;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * Utility class for hash functions.
 */
public class HashUtils {

    public static int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind, Object... objs) {
        return iterableQueryHash(hashKind, Arrays.asList(objs));
    }

    public static int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind, Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Enum) {
            return ((Enum)obj).name().hashCode();
        }
        if (obj instanceof Iterable<?>) {
            return iterableQueryHash(hashKind, (Iterable<?>)obj);
        }
        if (obj instanceof Object[]) {
            return queryHash(hashKind, (Object[])obj);
        }
        if (obj.getClass().isArray() && obj.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayHash(obj);
        }
        if (obj instanceof QueryHashable) {
            return ((QueryHashable)obj).queryHash(hashKind);
        }
        return obj.hashCode();
    }

    private static int iterableQueryHash(@Nonnull final QueryHashable.QueryHashKind hashKind, final Iterable<?> objects) {
        int result = 1;
        for (Object object : objects) {
            result = 31 * result + queryHash(hashKind, object);
        }
        return result;
    }

    private static int primitiveArrayHash(Object primitiveArray) {
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
