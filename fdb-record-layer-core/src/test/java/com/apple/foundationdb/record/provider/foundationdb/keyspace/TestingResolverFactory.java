/*
 * TestingResolverFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.google.common.cache.CacheStats;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

/**
 * A factory for generating {@code LocatableResolver}'s of a given type.  At the completion of
 * each test, any resolver created through the factory automatically has integrity validation
 * performed.
 */
public class TestingResolverFactory implements BeforeEachCallback, AfterEachCallback {
    public enum ResolverType {
        EXTENDED_DIRECTORY_LAYER,
        SCOPED_DIRECTORY_LAYER,
        SCOPED_INTERNING_LAYER
    }

    private final Set<LocatableResolver> resolvers = new HashSet<>();
    private final Set<ResolverValidator.ValidatedEntry> knownBadEntries = new HashSet<>();
    private final ResolverType defaultResolverType;
    private FDBDatabase database;

    public TestingResolverFactory(@Nonnull final ResolverType defaultResolverType) {
        this.defaultResolverType = defaultResolverType;
    }

    @Override
    public void beforeEach(@Nonnull final ExtensionContext context) throws Exception {
        resolvers.clear();
        knownBadEntries.clear();

        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.setDirectoryCacheSize(100);
        database = factory.getDatabase();
        // clear all state in the db
        database.close();
        // resets the lock cache
        database.setResolverStateRefreshTimeMillis(30000);

        wipeFDB();
    }

    @Override
    public void afterEach(@Nonnull final ExtensionContext context) throws Exception {
        for (LocatableResolver resolver : resolvers) {
            validate(resolver);
        }
        resolvers.clear();
    }

    @Nonnull
    public FDBDatabase getDatabase() {
        return database;
    }

    @Nonnull
    public CacheStats getDirectoryCacheStats() {
        return database.getDirectoryCacheStats();
    }

    @Nonnull
    public LocatableResolver getGlobalScope() {
        return getGlobalScope(this.database);
    }

    @Nonnull
    public LocatableResolver getGlobalScope(FDBDatabase database) {
        switch (defaultResolverType) {
        case EXTENDED_DIRECTORY_LAYER:
            return track(ExtendedDirectoryLayer.global(database));
        case SCOPED_DIRECTORY_LAYER:
            return track(ScopedDirectoryLayer.global(database));
        case SCOPED_INTERNING_LAYER:
            return track(ScopedInterningLayer.global(database));
        default:
            throw new IllegalStateException("Unknown resolver type: " + defaultResolverType);
        }
    }

    @Nonnull
    public LocatableResolver create(ResolvedKeySpacePath path) {
        switch (defaultResolverType) {
        case EXTENDED_DIRECTORY_LAYER:
            return track(new ExtendedDirectoryLayer(database, path));
        case SCOPED_DIRECTORY_LAYER:
            return track(new ScopedDirectoryLayer(database, path));
        case SCOPED_INTERNING_LAYER:
            return track(new ScopedInterningLayer(database, path));
        default:
            throw new IllegalStateException("Unknown resolver type: " + defaultResolverType);
        }
    }

    public ResolverValidator.ValidatedEntry deleteReverseEntry(LocatableResolver resolver, ResolverKeyValue keyValue) {
        try (FDBRecordContext context = resolver.getDatabase().openContext()) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_REVERSE_DIRECTORY_LOOKUP,
                resolver.deleteReverseForTesting(context, keyValue.getValue().getValue())
                        .thenCompose(ignore -> context.commitAsync()));
            database.clearReverseDirectoryCache();
        }

        final ResolverValidator.ValidatedEntry validatedEntry = new ResolverValidator.ValidatedEntry(
                ResolverValidator.ValidationResult.REVERSE_ENTRY_MISSING,
                keyValue);
        knownBadEntries.add(validatedEntry);
        return validatedEntry;
    }

    public ResolverValidator.ValidatedEntry putReverseEntry(LocatableResolver resolver, ResolverKeyValue forwardKeyValue, String wrongReverseKey) {
        try (FDBRecordContext context = resolver.getDatabase().openContext()) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_REVERSE_DIRECTORY_LOOKUP,
                resolver.putReverse(context, forwardKeyValue.getValue().getValue(), wrongReverseKey)
                        .thenCompose(ignore -> context.commitAsync()));
            database.clearReverseDirectoryCache();
        }

        final ResolverValidator.ValidatedEntry validatedEntry = new ResolverValidator.ValidatedEntry(
                        ResolverValidator.ValidationResult.REVERSE_ENTRY_MISMATCH,
                        forwardKeyValue,
                        wrongReverseKey);
        knownBadEntries.add(validatedEntry);
        return validatedEntry;
    }

    protected void validate(LocatableResolver resolver) {
        ResolverValidator.validate(
                null,
                resolver,
                ExecuteProperties.newBuilder()
                        .setFailOnScanLimitReached(false)
                        .setScannedRecordsLimit(500),
                10,
                true,
                (validatedEntry) -> {
                    if (!knownBadEntries.contains(validatedEntry)) {
                        throw new RuntimeException(resolver + ": Bad entry found: " + validatedEntry);
                    }
                });
    }

    private LocatableResolver track(LocatableResolver locatableResolver) {
        if (!resolvers.contains(locatableResolver)) {
            resolvers.add(locatableResolver);
        }
        return locatableResolver;
    }

    public void wipeFDB() {
        database.run((context -> {
            context.ensureActive().clear(new Range(new byte[] {(byte)0x00}, new byte[] {(byte)0xFF}));
            return null;
        }));
        database.clearCaches();
    }
}
