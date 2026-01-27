/*
 * RelationalKeyspaceProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePathWrapper;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * The Relational Keyspace.
 * "__SYS" in its current form, is a system schema that will house all the system tables.
 *
 *    / (root)
 *        |- __SYS (null)
 *        |
 *  [currently, we allow for the domains very naively for our use-cases like cli, testing, demo etc.]
 *        |- domain_name (string=domain_name)
 *            |- dbName (string)
 *                |- schema (string)
 */

@API(API.Status.EXPERIMENTAL)
public class RelationalKeyspaceProvider {

    public static final String SYS = "__SYS";
    public static final String CATALOG = "CATALOG";
    public static final String DB_NAME_DIR = "dbName";
    public static final String SCHEMA_DIR = "schema";
    public static final String INTERNING_LAYER = "__internedStrings";
    public static final String INTERNING_LAYER_VALUE = "IL";

    private static final RelationalKeyspaceProvider INSTANCE = new RelationalKeyspaceProvider();

    private final KeySpace keyspace;

    private RelationalKeyspaceProvider() {
        keyspace = new KeySpace(getSystemDirectory());
    }

    public static class RelationalDomainPath extends KeySpacePathWrapper {
        public RelationalDomainPath(@Nonnull KeySpacePath inner) {
            super(inner);
        }

        @Nonnull
        public String getDomainName() {
            return inner.getDirectoryName();
        }

        @Nonnull
        public RelationalDatabasePath database(@Nonnull String dbName) {
            Assert.thatUnchecked(INTERNING_LAYER.equals(dbName), ErrorCode.INVALID_DATABASE, () -> "Invalid database name: " + dbName);
            return (RelationalDatabasePath) inner.add(DB_NAME_DIR, dbName);
        }

        @Nonnull
        public KeySpacePath internedStrings() {
            return inner.add(INTERNING_LAYER);
        }

        /**
         * Generate a scope for interning strings for this domain. This can be used to shorten database
         * and schema names within the domain.
         *
         * @param context the FDB transaction context to use to resolve paths here
         * @return a future that will contain a {@link LocatableResolver} used by this domain
         */
        @Nonnull
        public CompletableFuture<LocatableResolver> generateScopeAsync(@Nonnull FDBRecordContext context) {
            return internedStrings().toResolvedPathAsync(context)
                    // Use ScopedInterningLayer here instead of a ScopedDirectoryLayer because it is more tailored to the job of shortening strings
                    .thenApply(resolvedPath -> new ScopedInterningLayer(context.getDatabase(), resolvedPath));
        }
    }

    public static class RelationalDatabasePath extends KeySpacePathWrapper {

        public RelationalDatabasePath(KeySpacePath inner) {
            super(inner);
        }

        @Nonnull
        public RelationalSchemaPath schemaPath(String schemaName) {
            return (RelationalSchemaPath) this.inner.add(RelationalKeyspaceProvider.SCHEMA_DIR, schemaName);
        }

        @Nonnull
        public URI toUri() {
            return KeySpaceUtils.pathToUri(inner);
        }
    }

    public static class RelationalSystemDomainPath extends RelationalDomainPath {
        public RelationalSystemDomainPath(KeySpacePath inner) {
            super(inner);
        }

        @Nonnull
        @Override
        public String getDomainName() {
            return SYS;
        }

        @Nonnull
        @Override
        public RelationalSystemDatabasePath database(@Nonnull String dbName) {
            Assert.thatUnchecked(SYS.equals(dbName), ErrorCode.UNDEFINED_DATABASE, "Unknown system database name: " + dbName);
            return (RelationalSystemDatabasePath) super.database(dbName);
        }
    }

    public static class RelationalSystemDatabasePath extends RelationalDatabasePath {

        public RelationalSystemDatabasePath(KeySpacePath inner) {
            super(inner);
        }

        @Override
        @Nonnull
        public RelationalSchemaPath schemaPath(String schemaName) {
            Assert.thatUnchecked(CATALOG.equals(schemaName), ErrorCode.UNDEFINED_SCHEMA, "Unknown system schema name: " + schemaName);
            return (RelationalSchemaPath) inner.add(CATALOG);
        }

        @Override
        @Nonnull
        public URI toUri() {
            return URI.create("/" + SYS);
        }
    }

    public static class RelationalSchemaPath extends KeySpacePathWrapper {

        public RelationalSchemaPath(KeySpacePath inner) {
            super(inner);
        }

        public String getSchemaName() {
            String directoryName = getDirectoryName();
            if (SCHEMA_DIR.equals(directoryName)) {
                return (String) getValue();
            } else {
                return directoryName;
            }
        }
    }

    private static KeySpaceDirectory getSystemDirectory() {
        return new KeySpaceDirectory(SYS, KeySpaceDirectory.KeyType.NULL, RelationalSystemDomainPath::new)
                .addSubdirectory(new KeySpaceDirectory(SYS, KeySpaceDirectory.KeyType.NULL, RelationalSystemDatabasePath::new)
                        .addSubdirectory(new KeySpaceDirectory(CATALOG, KeySpaceDirectory.KeyType.LONG, 0L, RelationalSchemaPath::new))
                )
                .addSubdirectory(new KeySpaceDirectory(INTERNING_LAYER, KeySpaceDirectory.KeyType.STRING, INTERNING_LAYER_VALUE));
    }

    public void registerDomainIfNotExists(@Nonnull String domainName) {
        final var keySpaceRoot = getKeySpace().getRoot();
        final var exists = keySpaceRoot.getSubdirectories().stream()
                .map(KeySpaceDirectory::getName)
                .anyMatch(dirName -> dirName.equals(domainName));
        if (exists) {
            return;
        }
        // Use the global (default) directory layer to intern the domain. This allows us to play nicer with
        // other data that is already on the cluster, which is mainly a concern during local testing. It also
        // helps with transitioning data over to the catalog if the original data was written using the global
        // directory layer to encode its first key space path element.
        final var domainDirectory = new DirectoryLayerDirectory(domainName, domainName, RelationalDomainPath::new)
                .addSubdirectory(new KeySpaceDirectory(INTERNING_LAYER, KeySpaceDirectory.KeyType.STRING, INTERNING_LAYER_VALUE));
        keySpaceRoot.addSubdirectory(domainDirectory);

        final var domainPath = (RelationalDomainPath) getKeySpace().path(domainName);
        domainDirectory.addSubdirectory(new DirectoryLayerDirectory(DB_NAME_DIR, RelationalDatabasePath::new, domainPath::generateScopeAsync, ResolverCreateHooks.getDefault())
                .addSubdirectory(new DirectoryLayerDirectory(SCHEMA_DIR, RelationalSchemaPath::new, domainPath::generateScopeAsync, ResolverCreateHooks.getDefault())));
    }

    public KeySpace getKeySpace() {
        return keyspace;
    }

    public RelationalKeyspaceProvider.RelationalDatabasePath toDatabasePath(@Nonnull URI url) throws RelationalException {
        return toDatabasePath(url, getKeySpace());
    }

    public static RelationalKeyspaceProvider.RelationalDatabasePath toDatabasePath(@Nonnull URI url, KeySpace keyspace) throws RelationalException {
        final var keySpacePath = KeySpaceUtils.toKeySpacePath(url, keyspace, true);
        // KeySpacePath is to the System database directory
        if (keySpacePath instanceof RelationalSystemDatabasePath) {
            return (RelationalSystemDatabasePath) keySpacePath;
        }
        // KeySpacePath is to the domain-specific database directory
        if (RelationalKeyspaceProvider.DB_NAME_DIR.equals(keySpacePath.getDirectoryName())) {
            return (RelationalKeyspaceProvider.RelationalDatabasePath) keySpacePath;
        }
        throw new RelationalException("<" + url + "> is an invalid database path", ErrorCode.INVALID_PATH);
    }

    @Nonnull
    public static RelationalKeyspaceProvider instance() {
        return INSTANCE;
    }

    @VisibleForTesting
    @Nonnull
    public static RelationalKeyspaceProvider newInstanceForTesting() {
        return new RelationalKeyspaceProvider();
    }
}
