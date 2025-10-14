/*
 * Tags.java
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

package com.apple.test;

/**
 * Annotation {@link org.junit.jupiter.api.Tag}s for Record Layer tests.
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public final class Tags {
    /**
     * Tests that require FoundationDB to be available.
     */
    public static final String RequiresFDB = "RequiresFDB";
    /**
     * Tests that are for performance investigations, and not for correctness validation.
     */
    public static final String Performance = "Performance";
    /**
     * Tests that take longer than 2 seconds for an individual test case.
     * <p>
     *     Note: the time here may be decreased in the future as more tests are added.
     * </p>
     * <p>
     *     Note: This tag does not actually enforce a timeout on the test (see issue #2454).
     * </p>
     */
    public static final String Slow = "Slow";
    /**
     * Tests that wipe the entire FDB cluster during their run.
     */
    public static final String WipesFDB = "WipesFDB";

    private Tags() {
    }

}
