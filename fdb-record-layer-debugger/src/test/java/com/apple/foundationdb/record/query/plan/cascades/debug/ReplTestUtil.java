/*
 * ReplTestUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Static test utilities for testing {@link PlannerRepl} functionality.
 */
final class ReplTestUtil {
    private ReplTestUtil() {
        // prevent instantiation
    }

    /**
     * Return a key and a value colored using the same colors used in {@link PlannerRepl} as a {@link String}.
     * @param key key to include in return string
     * @param value value to include in return string
     * @return a {@link String} with {@code key} and {@code value} concatenated with the expected colors.
     */
    static String coloredKeyValue(final String key, final String value) {
        return new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW + AttributedStyle.BRIGHT).bold())
                .append(key)
                .append(": ")
                .style(AttributedStyle.DEFAULT)
                .append(value).toAnsi();
    }
}
