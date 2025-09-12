/*
 * EmbeddedRelationalDriver.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@API(API.Status.EXPERIMENTAL)
public class EmbeddedRelationalDriver implements RelationalDriver {

    public static final String DRIVER_NAME = "Relational Embedded/Local JDBC Driver";

    public static final String JDBC_COLON = "jdbc:";
    public static final String JDBC_URL_PREFIX = JDBC_COLON + "embed:";

    private EmbeddedRelationalEngine engine;

    public EmbeddedRelationalDriver(@Nullable EmbeddedRelationalEngine engine) throws SQLException {
        this.engine = engine;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return connect(URI.create(url), Options.fromProperties(info));
    }

    @Override
    public RelationalConnection connect(@Nonnull URI url,
                                        @Nonnull Options connectionOptions) throws SQLException {
        return connect(url, null, connectionOptions);
    }

    @SuppressWarnings("PMD.CloseResource") // returns connection outliving auto-closeable object. Should consider refactoring
    public RelationalConnection connect(@Nonnull URI url,
                                        @Nullable Transaction existingTransaction,
                                        @Nonnull Options connectionOptions) throws SQLException {
        final var urlString = url.toString();
        if (!acceptsURL(urlString)) {
            return null;
        }
        url = URI.create(urlString.substring(JDBC_URL_PREFIX.length()));
        //first, we decide which cluster this database belongs to

        RelationalDatabase frl = null;
        try {
            for (StorageCluster cluster : engine.getStorageClusters()) {
                frl = cluster.loadDatabase(url, connectionOptions);
                if (frl != null) {
                    //we found the cluster!
                    break;
                }
            }
            if (frl == null) {
                String path = url.getPath();
                if (path == null) {
                    //this happens if they forget a leading / in the URL designator. Find the path by getting
                    //the string and removing the authority
                    path = url.toString();
                    if (url.getScheme() != null) {
                        path = path.replace(url.getScheme() + ":", "");
                    }
                    if (url.getAuthority() != null) {
                        path = path.replace(url.getAuthority() + ":", "");
                    }
                }
                throw new RelationalException("Database <" + path + "> does not exist", ErrorCode.UNDEFINED_DATABASE).toSqlException();
            }
            return frl.connect(existingTransaction);
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(JDBC_URL_PREFIX);
    }
}
