/*
 * KeyComparisons.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import java.util.Comparator;
import java.util.List;

/**
 * {@link Comparator}s for key expressions.
 */
@API(API.Status.UNSTABLE)
public class KeyComparisons {
    @SuppressWarnings("unchecked")
    public static final Comparator<Object> FIELD_COMPARATOR = (o1, o2) ->  {
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            } else {
                return -1;
            }
        } else if (o2 == null) {
            return 1;
        } else if (o1 instanceof byte[]) {
            return ByteArrayUtil.compareUnsigned((byte[])o1, (byte[])o2);
        } else if (o1 instanceof List) {
            return KeyComparisons.KEY_COMPARATOR.compare((List<Object>)o1, (List<Object>)o2);
        } else {
            return ((Comparable)o1).compareTo(o2);
        }
    };
    public static final Comparator<List<Object>> KEY_COMPARATOR = (l1, l2) -> {
        for (int i = 0; ; i++) {
            if (i >= l1.size()) {
                if (i >= l2.size()) {
                    return 0;
                } else {
                    return -1;
                }
            } else if (i >= l2.size()) {
                return 1;
            }
            int compare = FIELD_COMPARATOR.compare(l1.get(i), l2.get(i));
            if (compare != 0) {
                return compare;
            }
        }
    };

    private KeyComparisons() {
    }
}
