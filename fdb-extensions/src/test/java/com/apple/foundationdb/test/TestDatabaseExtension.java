/*
 * FDBExtension.java
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

package com.apple.foundationdb.test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;

/**
 * Test extension to use to get a connection to an FDB {@link Database}. This handles setting up the FDB
 * network including setting the API version and then returning the database. Tests should generally
 * create an instance of this as static member variable and then register the class as an extension so
 * that the callbacks associated with this extension are executed. Like so:
 *
 * <pre>{@code
 *     @RegisterExtension
 *     static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
 * }</pre>
 *
 * <p>
 * The user can then call {@link #getDatabase() dbExtension.getDatabase()} to get an actual FDB handle.
 * </p>
 */
public class TestDatabaseExtension implements BeforeAllCallback, AfterAllCallback {
    private static final int MIN_API_VERSION = 630;
    private static final int MAX_API_VERSION = 710;
    private static final String API_VERSION_PROPERTY = "com.apple.foundationdb.apiVersion";

    private final boolean trace;
    private Database db;

    public TestDatabaseExtension() {
        this(false);
    }

    public TestDatabaseExtension(boolean trace) {
        this.trace = trace;
    }

    public static int getAPIVersion() {
        int apiVersion = Integer.parseInt(System.getProperty(API_VERSION_PROPERTY, "630"));
        if (apiVersion < MIN_API_VERSION || apiVersion > MAX_API_VERSION) {
            throw new IllegalStateException(String.format("unsupported API version %d (must be between %d and %d)",
                    apiVersion, MIN_API_VERSION, MAX_API_VERSION));
        }
        return apiVersion;
    }

    @Override
    public void beforeAll(final ExtensionContext extensionContext) {
        if (!FDB.isAPIVersionSelected()) {
            FDB fdb = FDB.selectAPIVersion(getAPIVersion());
            if (trace) {
                NetworkOptions options = fdb.options();
                options.setTraceEnable("/tmp");
                options.setTraceLogGroup("fdb_extensions_tests");
            }
            fdb.setUnclosedWarning(true);
        }
    }

    @Nonnull
    public Database getDatabase() {
        if (db == null) {
            db = FDB.instance().open();
        }
        return db;
    }

    @Override
    public void afterAll(final ExtensionContext extensionContext) {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
