/*
 * FDBDatabaseFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * A singleton maintaining a list of {@link FDBDatabase} instances, indexed by their cluster file location.
 */
@API(API.Status.STABLE)
public class FDBDatabaseFactoryImpl extends FDBDatabaseFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseFactory.class);
    private static final int API_VERSION = 630;

    @Nonnull
    private static final FDBDatabaseFactoryImpl INSTANCE = new FDBDatabaseFactoryImpl();

    /**
     * When set to true, static options have been set on the FDB instance.
     * Made volatile because multiple  {@link FDBDatabaseFactory} instances can be created technically, and thus can be
     * racy
     * during init.
     * <p>
     * This option only applies (currently) to {@link FDBDatabaseFactoryImpl} and is present on the abstract class for
     * backwards compatibility, it'll be moved there permanently in the next major release.
     */
    protected static volatile boolean staticOptionsSet = false;

    /**
     * Made volatile because multiple {@link FDBDatabaseFactory} instances can be created technically, and thus can be
     * racy during init.
     * <p>
     * Default is 1, which is basically disabled.
     * <p>
     * This option only applies (currently) to {@link FDBDatabaseFactoryImpl} and is present on the abstract class for
     * backwards compatibility, it'll be moved there permanently in the next major release.
     */
    protected static volatile int threadsPerClientVersion = 1;

    @Nonnull
    private FDBLocalityProvider localityProvider = FDBLocalityUtil.instance();

    @Nullable
    private FDB fdb;
    private boolean inited;

    @Nullable
    private String traceDirectory = null;
    @Nullable
    private String traceLogGroup = null;
    @Nonnull
    private FDBTraceFormat traceFormat = FDBTraceFormat.DEFAULT;

    private boolean runLoopProfilingEnabled = false;

    /**
     * The default is a log-based predicate, which can also be used to enable tracing on a more granular level
     * (such as by request) using {@link #setTransactionIsTracedSupplier(Supplier)}.
     */
    @Nonnull
    private Supplier<Boolean> transactionIsTracedSupplier = LOGGER::isTraceEnabled;

    @Nonnull
    public static FDBDatabaseFactoryImpl instance() {
        return INSTANCE;
    }

    protected synchronized FDB initFDB() {
        if (!inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Starting FDB"));
            }
            fdb = FDB.selectAPIVersion(API_VERSION);
            fdb.setUnclosedWarning(unclosedWarning);
            setStaticOptions(fdb);
            NetworkOptions options = fdb.options();
            if (!traceFormat.isDefaultValue()) {
                options.setTraceFormat(traceFormat.getOptionValue());
            }
            if (traceDirectory != null) {
                options.setTraceEnable(traceDirectory);
            }
            if (traceLogGroup != null) {
                options.setTraceLogGroup(traceLogGroup);
            }
            if (runLoopProfilingEnabled) {
                options.setEnableRunLoopProfiling();
            }
            if (networkExecutor == null) {
                fdb.startNetwork();
            } else {
                fdb.startNetwork(networkExecutor);
            }
            inited = true;
        }
        return fdb;
    }

    private static synchronized void setStaticOptions(final FDB fdb) {
        /*
         * There are a few FDB settings that have to be set statically, but also need to have room
         * for configuration. For the most part, FDBDatabaseFactory is a singleton and so in _theory_ this shouldn't
         * matter. However, in practice it is possible to create multiple factories(i.e. in test code and such),
         * and doing so may cause problems with these settings (errors thrown, that kind of thing). To avoid that,
         * we have to follow a somewhat goofy pattern of making those settings static, and checking to ensure that
         * we only set those options once. This block of code does that.
         *
         * Note that this method is synchronized on the class; this is so that multiple concurrent attempts to
         * init an FDBDatabaseFactory won't cause this function to fail halfway through.
         */
        if (!staticOptionsSet) {
            fdb.options().setClientThreadsPerVersion(threadsPerClientVersion);

            staticOptionsSet = true;
        }
    }

    @Override
    public synchronized void shutdown() {
        if (inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Shutting down FDB"));
            }
            for (FDBDatabase database : databases.values()) {
                database.close();
            }
            // TODO: Does this do the right thing yet?
            fdb.stopNetwork();
            inited = false;
        }
    }

    @Override
    @SpotBugsSuppressWarnings("IS2_INCONSISTENT_SYNC")
    public void setTrace(@Nullable String traceDirectory, @Nullable String traceLogGroup) {
        this.traceDirectory = traceDirectory;
        this.traceLogGroup = traceLogGroup;
    }

    @Override
    public void setTraceFormat(@Nonnull FDBTraceFormat traceFormat) {
        this.traceFormat = traceFormat;
    }

    @Override
    public synchronized void setRunLoopProfilingEnabled(boolean runLoopProfilingEnabled) {
        if (inited) {
            throw new RecordCoreException("run loop profiling can not be enabled as the client has already started");
        }
        this.runLoopProfilingEnabled = runLoopProfilingEnabled;
    }

    @Override
    public boolean isRunLoopProfilingEnabled() {
        return runLoopProfilingEnabled;
    }

    // TODO: Demote these to UNSTABLE and deprecate at some point.

    @Override
    public void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier) {
        this.transactionIsTracedSupplier = transactionIsTracedSupplier;
    }

    @Override
    public Supplier<Boolean> getTransactionIsTracedSupplier() {
        return transactionIsTracedSupplier;
    }

    @Override
    @Nonnull
    public synchronized FDBDatabase getDatabase(@Nullable String clusterFile) {
        FDBDatabase database = databases.get(clusterFile);
        if (database == null) {
            database = new FDBDatabase(this, clusterFile);
            database.setDirectoryCacheSize(getDirectoryCacheSize());
            database.setTrackLastSeenVersion(getTrackLastSeenVersion());
            database.setResolverStateRefreshTimeMillis(getStateRefreshTimeMillis());
            database.setDatacenterId(getDatacenterId());
            database.setStoreStateCache(storeStateCacheFactory.getCache(database));
            databases.put(clusterFile, database);
        }
        return database;
    }

    @Override
    @Nonnull
    public FDBLocalityProvider getLocalityProvider() {
        return localityProvider;
    }

    @Override
    public void setLocalityProvider(@Nonnull FDBLocalityProvider localityProvider) {
        this.localityProvider = localityProvider;
    }

    /**
     * Set the number of threads per FDB client version. The default value is 1.
     *
     * @param threadsPerClientV the number of threads per client version. Cannot be less than 1.
     */
    public static void setThreadsPerClientVersion(int threadsPerClientV) {
        if (staticOptionsSet) {
            throw new RecordCoreException("threads per client version cannot be changed as the version has already been initiated");
        }
        if (threadsPerClientV < 1) {
            //if the thread count is too low, disable the setting
            threadsPerClientV = 1;
        }
        threadsPerClientVersion = threadsPerClientV;
    }

    public static int getThreadsPerClientVersion() {
        return threadsPerClientVersion;
    }

    @Nonnull
    @Override
    public Database open(final String clusterFile) {
        FDB fdb = initFDB();
        return fdb.open(clusterFile);
    }
}
