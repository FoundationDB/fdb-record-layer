/*
 * RelationalKeyspaceProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePathWrapper;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.net.URI;

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

public class RelationalKeyspaceProvider {

    public static final String SYS = "__SYS";
    public static final String CATALOG = "CATALOG";
    public static final String DB_NAME_DIR = "dbName";
    public static final String SCHEMA_DIR = "schema";

    private static KeySpace keyspace;

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

    public static class RelationalSystemDatabasePath extends RelationalDatabasePath {

        public RelationalSystemDatabasePath(KeySpacePath inner) {
            super(inner);
        }

        @Override
        @Nonnull
        public RelationalSchemaPath schemaPath(String schemaName) {
            Assert.thatUnchecked(schemaName.equals(CATALOG), "Unknown system schema name: " + schemaName, ErrorCode.UNDEFINED_SCHEMA);
            return new RelationalSchemaPath(inner);
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
    }

    private static KeySpaceDirectory getSystemDirectory() {
        return new KeySpaceDirectory(SYS, KeySpaceDirectory.KeyType.NULL, RelationalSystemDatabasePath::new);
    }

    public static void registerDomainIfNotExists(@Nonnull String domainName) {
        final var keySpaceRoot = getKeySpace().getRoot();
        final var exists = keySpaceRoot.getSubdirectories().stream()
                .map(KeySpaceDirectory::getName)
                .anyMatch(dirName -> dirName.equals(domainName));
        if (exists) {
            return;
        }
        keySpaceRoot.addSubdirectory(
                new KeySpaceDirectory(domainName, KeySpaceDirectory.KeyType.STRING, domainName).addSubdirectory(
                        new KeySpaceDirectory(DB_NAME_DIR, KeySpaceDirectory.KeyType.STRING, RelationalDatabasePath::new)
                                .addSubdirectory(new KeySpaceDirectory(SCHEMA_DIR, KeySpaceDirectory.KeyType.STRING, RelationalSchemaPath::new))));
    }

    public static KeySpace getKeySpace() {
        if (keyspace == null) {
            keyspace = new KeySpace(getSystemDirectory());
        }
        return keyspace;
    }

    public static RelationalKeyspaceProvider.RelationalDatabasePath toDatabasePath(@Nonnull URI url, KeySpace keyspace) throws RelationalException {
        final var keySpacePath = KeySpaceUtils.toKeySpacePath(url, keyspace);
        // KeySpacePath is to the System database directory
        if (keySpacePath instanceof RelationalSystemDatabasePath) {
            return (RelationalSystemDatabasePath) keySpacePath;
        }
        // KeySpacePath is to the domain-specific database directory
        if (keySpacePath.getDirectoryName().equals(RelationalKeyspaceProvider.DB_NAME_DIR)) {
            return (RelationalKeyspaceProvider.RelationalDatabasePath) keySpacePath;
        }
        throw new RelationalException("<" + url + "> is an invalid database path", ErrorCode.INVALID_PATH);
    }
}
