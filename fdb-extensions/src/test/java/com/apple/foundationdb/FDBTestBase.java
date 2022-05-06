/*
 * FDBTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb;

import org.junit.jupiter.api.BeforeAll;

/**
 * Base class that all tests touching FoundationDB should inherit from.
 */
public abstract class FDBTestBase {
    private static final int MIN_API_VERSION = 630;
    private static final int MAX_API_VERSION = 710;
    private static final String API_VERSION_PROPERTY = "com.apple.foundationdb.apiVersion";

    private static int getAPIVersion() {
        int apiVersion = Integer.parseInt(System.getProperty(API_VERSION_PROPERTY, "630"));
        if (apiVersion < MIN_API_VERSION || apiVersion > MAX_API_VERSION) {
            throw new IllegalStateException(String.format("unsupported API version %d (must be between %d and %d)",
                    apiVersion, MIN_API_VERSION, MAX_API_VERSION));
        }
        return apiVersion;
    }

    @BeforeAll
    public static void setupFDB() {
        FDB fdb = FDB.selectAPIVersion(getAPIVersion());
        fdb.setUnclosedWarning(true);
    }
}
