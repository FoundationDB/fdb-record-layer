/*
 * TextCollator.java
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

package com.apple.foundationdb.record.provider.common.text;

import com.apple.foundationdb.annotation.API;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.util.Comparator;

/**
 * An interface for locale-sensitive text comparison and sorting.
 *
 * A text collator is specified with a locale name and a strength.
 *
 * @see TextCollatorRegistry
 */
@API(API.Status.EXPERIMENTAL)
public interface TextCollator extends Comparator<String> {
    /**
     * Standard strength values.
     *
     * These match values in {@code java.text.Collator} and other APIs.
     */
    final class Strength {
        /**
         * Respect <em>primary</em> differences, which normally means base form.
         */
        public static final int PRIMARY = 0;

        /**
         * Respect <em>secondary</em> differences, which normally means accents.
         */
        public static final int SECONDARY = 1;

        /**
         * Respect <em>tertiary</em> differences, which normally means case.
         */
        public static final int TERTIARY = 2;

        private Strength() {
        }
    }

    /**
     * Compare the two strings according to the this collator's collation rules.
     *
     * @param str1 the first string
     * @param str2 the second string
     * @return an integer equal to zero if the two strings are equivalent, less than zero if the first string
     * should sort before the second, or greater than zero if the first string should sort after the second.
     */
    @Override
    int compare(@Nonnull String str1, @Nonnull String str2);

    /**
     * Get a representation of a string suitable for storing in an index for this collation.
     *
     * The unsigned byte comparison of the result byte array will match this collator's ordering.
     * @param str the string to index
     * @return a byte string for storing in an index
     */
    @Nonnull
    ByteString getKey(@Nonnull String str);
}
