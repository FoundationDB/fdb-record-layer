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

package com.apple.foundationdb.record.provider.foundationdb;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class from which all FDB tests should be derived.
 */
public abstract class FDBTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBTestBase.class);

    public static final String BLOCKING_IN_ASYNC_PROPERTY = "com.apple.foundationdb.record.blockingInAsyncDetection";

    @BeforeAll
    public static void setupBlockingInAsyncDetection() {
        final String str = System.getProperty(BLOCKING_IN_ASYNC_PROPERTY);
        if (str != null) {
            final BlockingInAsyncDetection detection;
            try {
                detection = BlockingInAsyncDetection.valueOf(str);
            } catch (Exception e) {
                LOGGER.error("Illegal value provided for " + BLOCKING_IN_ASYNC_PROPERTY + ": " + str);
                return;
            }
            FDBDatabaseFactory.instance().setBlockingInAsyncDetection(detection);
            if (detection != BlockingInAsyncDetection.DISABLED) {
                LOGGER.info("Blocking-in-async is " + detection);
            }
        }
    }
}
