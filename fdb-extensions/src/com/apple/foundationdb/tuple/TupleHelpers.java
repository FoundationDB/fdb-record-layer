/*
 * TupleHelpers.java
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

package com.apple.foundationdb.tuple;

import java.util.List;

/**
 * Helper methods for working with {@link Tuple}s.
 */
public class TupleHelpers {

    public static final Tuple EMPTY = Tuple.from();

    public static Tuple set(Tuple src, int index, Object value) {
        final List<Object> items = src.getItems();
        items.set(index, value);
        return Tuple.fromList(items);
    }

    public static Tuple subTuple(Tuple src, int start, int end) {
        final List<Object> items = src.getItems();
        return Tuple.fromList(items.subList(start, end));
    }

    /**
     * Compare two tuples lexicographically, that is, the same way they would sort when used as keys.
     *
     * Note that this is currently WAY more efficient than calling {@link Tuple#equals},
     * and slightly more efficient than calling {@link Tuple#compareTo}.
     * @param t1 the first {@link Tuple} to compare
     * @param t2 the second {@link Tuple} to compare
     * @return {@code 0} if {@code t1} and {@code t2} are equal
     *         a value less than {@code 0} if {@code t1} would sort before {@code t2}
     *         a value greater than {@code 0} if {@code t1} would sort after {@code t2}
     */
    public static int compare(Tuple t1, Tuple t2) {
        final int t1Len = t1.size();
        final int t2Len = t2.size();
        final int len = Math.min(t1Len, t2Len);

        for (int i = 0; i < len; i++) {
            int rc = TupleUtil.compareItems(t1.get(i), t2.get(i));
            if (rc != 0) {
                return rc;
            }
        }
        return t1Len - t2Len;
    }
}
