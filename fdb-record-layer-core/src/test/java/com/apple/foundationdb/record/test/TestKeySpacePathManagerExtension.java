/*
 * TestKeySpacePathExtension.java
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A wrapper around the {@link TestKeySpacePathManager} that closes the path manager automatically
 * when the test has ended. To ensure the callbacks get run, the test fixture should define a
 * {@link TestKeySpacePathManagerExtension} member variable and then register the extension, like so:
 *
 * <pre>{@code
 *     @RegisterExtension
 *     static final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
 *     @RegisterExtension
 *     final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
 * }</pre>
 *
 * <p>
 * Tests within the fixture can then allocate paths via the {@code pathManager} extension that will
 * be automatically cleaned up after the test completes.
 * </p>
 *
 * @see TestKeySpacePathManager
 * @see FDBDatabaseExtension
 */
public class TestKeySpacePathManagerExtension implements AfterEachCallback {
    @Nonnull
    private final FDBDatabaseExtension dbExtension;
    @Nullable
    private TestKeySpacePathManager pathManager;

    public TestKeySpacePathManagerExtension(@Nonnull FDBDatabaseExtension dbExtension) {
        this.dbExtension = dbExtension;
    }

    @Nonnull
    public TestKeySpacePathManager getPathManager() {
        if (pathManager == null) {
            pathManager = new TestKeySpacePathManager(dbExtension.getDatabase());
        }
        return pathManager;
    }

    @Nonnull
    public KeySpacePath createPath(String... pathElems) {
        return getPathManager().createPath(pathElems);
    }

    @Override
    public void afterEach(final ExtensionContext extensionContext) {
        if (pathManager != null) {
            pathManager.close();
            pathManager = null;
        }
    }
}
