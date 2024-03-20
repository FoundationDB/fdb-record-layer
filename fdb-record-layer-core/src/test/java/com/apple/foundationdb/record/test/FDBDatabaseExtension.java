/*
 * FDBDatabaseFactoryExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.test;

import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.BlockingInAsyncDetection;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactoryImpl;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extension that allows the user to specify the database. It ensures that FDB has been properly initialized
 * and that the {@link FDBDatabase} object used during a test does not leak between test runs. This registers
 * call backs that run before and after tests, so it is suggested that users use the {@link org.junit.jupiter.api.extension.RegisterExtension}
 * annotation to ensure that those callbacks run. Like so:
 *
 * <pre>{@code
 *     @RegisterExtension
 *     static final FDBDatabaseExtension = new FDBDatabaseExtension();
 * }</pre>
 */
public class FDBDatabaseExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseExtension.class);
    public static final String BLOCKING_IN_ASYNC_PROPERTY = "com.apple.foundationdb.record.blockingInAsyncDetection";
    public static final String API_VERSION_PROPERTY = "com.apple.foundationdb.apiVersion";
    @Nullable
    private FDBDatabase db;

    public FDBDatabaseExtension() {
    }

    public APIVersion getAPIVersion() {
        String apiVersionStr = System.getProperty(API_VERSION_PROPERTY);
        if (apiVersionStr == null) {
            return APIVersion.getDefault();
        }
        return APIVersion.fromVersionNumber(Integer.parseInt(apiVersionStr));
    }

    public void initFDB() {
        FDBDatabaseFactoryImpl factory = FDBDatabaseFactory.instance();
        factory.setAPIVersion(getAPIVersion());
        factory.setUnclosedWarning(true);
    }

    public void setupBlockingInAsyncDetection() {
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

    @Override
    public void beforeAll(final ExtensionContext extensionContext) {
        setupBlockingInAsyncDetection();
        initFDB();
    }

    @Override
    public void beforeEach(final ExtensionContext extensionContext) {
        if (db != null) {
            db.close();
            db = null;
        }
        FDBDatabaseFactory.instance().clear();
    }

    @Nonnull
    public FDBDatabase getDatabase() {
        if (db == null) {
            db = FDBDatabaseFactory.instance().getDatabase();
        }
        return db;
    }

    @Override
    public void afterEach(final ExtensionContext extensionContext) {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
