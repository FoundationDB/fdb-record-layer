/*
 * TestKeySpacePathManager.java
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

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Manager that is used to create key spaces for tests. This will create a new path every time
 * {@link #createPath(String...)} is called, so tests requiring multiple key space paths can
 * make multiple calls to this method to generate multiple paths. To ensure that these paths
 * get cleaned up at the end of the test, the path manager should be closed. This can be
 * done by calling {@link #close()} on the object or by using it within a {@code try}-with-resources
 * block, but many tests may find it easier to use the {@link TestKeySpacePathManagerExtension},
 * which will result in the path manager being closed in the after-each callback.
 *
 * @see TestKeySpacePathManagerExtension
 */
public class TestKeySpacePathManager implements AutoCloseable {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(TestKeySpacePathManager.class);
    @Nonnull
    private static final KeySpacePath BASE_PATH = TestKeySpace.keySpace.path("record-test")
            .add("unit");
    @Nonnull
    private final FDBDatabase db;
    @Nonnull
    private final Set<KeySpacePath> paths;
    private boolean closed;

    public TestKeySpacePathManager(@Nonnull FDBDatabase db) {
        this.db = db;
        this.paths = new HashSet<>();
    }

    /**
     * Create a new key space path. It will be assigned to a unique path that should
     * not be shared by any other test.
     *
     * @param pathElements a list of path elements to apply to the end of the test key space path
     * @return a new {@link KeySpacePath} suffixed by the given path elements
     */
    @Nonnull
    public KeySpacePath createPath(String... pathElements) {
        if (closed) {
            throw new RecordCoreException("cannot create path with closed key space manager");
        }

        // Generate a new path with a random UUID
        KeySpacePath path = BASE_PATH.add(TestKeySpace.TEST_UUID, UUID.randomUUID().toString());
        for (String pathElement : pathElements) {
            path = path.add(pathElement);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("creating test key space path",
                    LogMessageKeys.KEY_SPACE_PATH, path));
        }
        assertTrue(paths.add(path), "UUID collision");

        // Eagerly resolve the path under a JVM-wide lock. Path resolution goes through
        // FDBDatabase.resolverStateCache, an AsyncLoadingCache with a hardcoded 5-second
        // deadline (AsyncLoadingCache.DEFAULT_DEADLINE_TIME_MILLIS). When many parallel
        // tests resolve directory-layer entries concurrently, the FDB-side work appears
        // to contend with itself (retries triggered by conflicting commits on the
        // resolver state) and the 5-second deadline fires from random call sites in
        // test bodies and afterEach hooks. Serialising the resolution here means the
        // resolver-state cache is warm by the time the test actually uses the path,
        // so KeySpacePath.toTuple / FDBRecordStore.createOrOpen / deleteAllData calls
        // hit the cache instead of racing on directory-layer reads.
        //
        // Use db.newRunner so we get the runner's standard retry loop for free.
        // DeadlineExceededException extends LoggableException (not FDBException nor
        // RecordCoreRetriableTransactionException) so the runner doesn't consider it
        // retriable by default; wrap it explicitly to opt in.
        //
        // TODO: investigate whether the directory layer / LocatableResolver has a bug
        // where concurrent resolutions of distinct keys fail to make progress on
        // retry (rather than just being slow). If yes, the production-side fix would
        // remove the need for this workaround entirely. See e.g.
        // FDBDatabase.resolverStateCache, LocatableResolver.loadResolverState.
        synchronized (TestKeySpacePathManager.class) {
            final FDBRecordContextConfig.Builder config = FDBRecordContextConfig.newBuilder()
                    .setTransactionId("pathManager_createPath_" + UUID.randomUUID())
                    .setLogTransaction(true)
                    .setMdcContext(MDC.getCopyOfContextMap());
            try (FDBDatabaseRunner runner = db.newRunner(config)) {
                final KeySpacePath finalPath = path;
                runner.run(context -> {
                    try {
                        finalPath.toTuple(context);
                    } catch (MoreAsyncUtil.DeadlineExceededException e) {
                        // The 5-second AsyncLoadingCache deadline occasionally fires
                        // under heavy parallel test load even though the work would
                        // succeed on retry. Wrap so the runner's retry loop picks it up.
                        throw new RecordCoreRetriableTransactionException(
                                "deadline exceeded resolving test key space path", e);
                    }
                    return null;
                });
            }
        }

        return path;
    }

    /**
     * Delete all the data in any of the paths created by this path manager.
     */
    @Override
    public void close() {
        if (!closed) {
            if (!paths.isEmpty()) {
                final FDBRecordContextConfig.Builder config = FDBRecordContextConfig.newBuilder()
                        .setTransactionId("pathManager_" + UUID.randomUUID())
                        .setLogTransaction(true)
                        .setMdcContext(MDC.getCopyOfContextMap());
                try (FDBDatabaseRunner runner = db.newRunner(config)) {
                    runner.run(context -> {
                        try {
                            for (KeySpacePath path : paths) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(KeyValueLogMessage.of("deleting test key space path",
                                            LogMessageKeys.KEY_SPACE_PATH, path.toString(path.toTuple(context))));
                                }
                                path.deleteAllData(context);
                            }
                        } catch (MoreAsyncUtil.DeadlineExceededException e) {
                            // Same reason as in createPath: the 5-second AsyncLoadingCache
                            // deadline on FDBDatabase.resolverStateCache occasionally fires
                            // under heavy parallel test load. Wrap as a retriable exception
                            // so the runner's retry loop picks it up instead of failing the
                            // afterEach cleanup.
                            throw new RecordCoreRetriableTransactionException(
                                    "deadline exceeded resolving test key space path during cleanup", e);
                        }
                        return null;
                    });
                }
            }
            closed = true;
        }
    }
}
