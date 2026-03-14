/*
 * RelationalKeyspaceProviderTest.java
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Tests of the {@link RelationalKeyspaceProvider} ensuring that database URIs can be turned into key space paths
 * that make sense.
 */
public class RelationalKeyspaceProviderTest {
    @Nonnull
    private static final String defaultDomain = "DEFAULT_DOMAIN";
    @Nonnull
    private final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.newInstanceForTesting();

    private void registerDefaultDomain() {
        keyspaceProvider.registerDomainIfNotExists(defaultDomain);
    }

    @Test
    void accessSysDomain() throws RelationalException {
        URI uri = URI.create("/" + RelationalKeyspaceProvider.SYS);
        var databasePath = keyspaceProvider.toDatabasePath(uri);
        Assertions.assertThat(databasePath)
                .isInstanceOf(RelationalKeyspaceProvider.RelationalSystemDatabasePath.class)
                .isEqualTo(keyspaceProvider.getKeySpace().path(RelationalKeyspaceProvider.SYS).add(RelationalKeyspaceProvider.SYS));
        Assertions.assertThat(databasePath.toUri())
                .isEqualTo(uri);
    }

    @Test
    void accessCatalog() throws RelationalException {
        URI uri = URI.create("/" + RelationalKeyspaceProvider.SYS);
        var databasePath = keyspaceProvider.toDatabasePath(uri);
        var catalogSchemaPath = databasePath.schemaPath(RelationalKeyspaceProvider.CATALOG);
        Assertions.assertThat(catalogSchemaPath)
                .isInstanceOf(RelationalKeyspaceProvider.RelationalSchemaPath.class)
                .isEqualTo(keyspaceProvider.getKeySpace().path(RelationalKeyspaceProvider.SYS).add(RelationalKeyspaceProvider.SYS).add(RelationalKeyspaceProvider.CATALOG));
    }

    @Test
    void databaseInDomain() throws RelationalException {
        registerDefaultDomain();
        URI uri = URI.create("/" + defaultDomain + "/theDatabase");
        var dbPath = keyspaceProvider.toDatabasePath(uri);
        var expectedPath = keyspaceProvider.getKeySpace().path(defaultDomain)
                .add(RelationalKeyspaceProvider.DB_NAME_DIR, "theDatabase");
        Assertions.assertThat(dbPath)
                .isEqualTo(expectedPath);
        Assertions.assertThat(dbPath.toUri())
                .isEqualTo(uri);
    }

    @Test
    void schemaInDomain() throws RelationalException {
        registerDefaultDomain();
        URI url = URI.create("/" + defaultDomain + "/theDatabase/theSchema");
        var schemaPath = KeySpaceUtils.toKeySpacePath(url, keyspaceProvider.getKeySpace());
        Assertions.assertThat(schemaPath)
                .isInstanceOf(RelationalKeyspaceProvider.RelationalSchemaPath.class)
                .isEqualTo(keyspaceProvider.getKeySpace().path(defaultDomain).add(RelationalKeyspaceProvider.DB_NAME_DIR, "theDatabase").add(RelationalKeyspaceProvider.SCHEMA_DIR, "theSchema"));
        Assertions.assertThat(((RelationalKeyspaceProvider.RelationalSchemaPath) schemaPath).getSchemaName())
                .isEqualTo("theSchema");
    }

    @Test
    void defaultSchemaInDomain() throws RelationalException {
        registerDefaultDomain();
        URI url = URI.create("/" + defaultDomain + "/theDatabase/");
        var schemaPath = KeySpaceUtils.toKeySpacePath(url, keyspaceProvider.getKeySpace());
        Assertions.assertThat(schemaPath)
                .isInstanceOf(RelationalKeyspaceProvider.RelationalSchemaPath.class)
                .isEqualTo(keyspaceProvider.getKeySpace().path(defaultDomain)
                        .add(RelationalKeyspaceProvider.DB_NAME_DIR, "theDatabase")
                        .add(RelationalKeyspaceProvider.DEFAULT_SCHEMA_DIR));
    }
}
