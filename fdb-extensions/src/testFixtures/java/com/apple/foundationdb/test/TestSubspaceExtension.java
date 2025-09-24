/*
 * TestSubspaceExtension.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Extension for creating a subspace for tests. Each test will be given a unique subspace, and the data
 * will be cleared out during the {@link AfterEachCallback} for this extension. To use, create a member variable
 * of the test and register the extension:
 *
 * <pre>{@code
 *     @RegisterExtension
 *     static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
 *     @RegisterExtension
 *     TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);
 * }</pre>
 *
 * <p>
 * Within the test, call {@link #getSubspace()} to get the test's allocated exception. As long as all usage
 * for that test goes through the extension, the data will be deleted after the test has completed.
 * </p>
 *
 * @see TestDatabaseExtension
 */
public class TestSubspaceExtension implements AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSubspaceExtension.class);
    private final TestDatabaseExtension dbExtension;
    @Nullable
    private Subspace subspace;

    public TestSubspaceExtension(TestDatabaseExtension dbExtension) {
        this.dbExtension = dbExtension;
    }

    @Nonnull
    public Subspace getSubspace() {
        if (subspace == null) {
            subspace = dbExtension.getDatabase().runAsync(tr ->
                DirectoryLayer.getDefault().createOrOpen(tr, List.of("fdb-extensions-test"))
                        .thenApply(directorySubspace -> directorySubspace.subspace(Tuple.from(UUID.randomUUID())))
            ).join();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("created test subspace subspace=\"{}\"", ByteArrayUtil2.loggable(subspace.getKey()));
            }
        }
        return subspace;
    }

    @Override
    public void afterEach(final ExtensionContext extensionContext) {
        if (subspace != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("clearing test subspace subspace=\"{}\"", ByteArrayUtil2.loggable(subspace.getKey()));
            }
            dbExtension.getDatabase().run(tx -> {
                tx.clear(Range.startsWith(subspace.pack()));
                return null;
            });
            subspace = null;
        }
    }
}
