/*
 * JDBCRelationalDriver.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * JDBC Relational Driver.
 * Use mysql/postgres URL format; i.e. jdbc:relational://HOST[:PORT].
 */
@SuppressWarnings({"PMD.SystemPrintln"}) // Used in extreme when a failure to register Driver
// TODO: Implement RelationalDriver after it inherits from Driver.
@API(API.Status.EXPERIMENTAL)
public class JDBCRelationalDriver implements RelationalDriver {
    /**
     * Needed by {@link JDBCRelationalDatabaseMetaData#getDriverName()}.
     */
    static final String DRIVER_NAME = "Relational JDBC Driver";

    // Load this driver and register it with the DriverManager.
    // From Code Example 9-1 in the JDBC 4.3 spec.
    static {
        try {
            DriverManager.registerDriver(new JDBCRelationalDriver());
        } catch (SQLException sqlException) {
            System.err.println(sqlException);
        }
    }

    @Override
    public RelationalConnection connect(String url, Properties info) throws SQLException {
        return connect(URI.create(url), Options.fromProperties(info));
    }

    @Override
    public RelationalConnection connect(@Nonnull URI url,
                                        @Nonnull Options connectionOptions) throws SQLException {
        final var urlString = url.toString();
        if (!acceptsURL(urlString)) {
            return null;
        }
        // Parse the url String as a URI; makes it easy to pull out the pieces.
        URI uri;
        try {
            uri = new URI(urlString.substring(JDBCURI.JDBC_URL_PREFIX.length()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        if (!uri.getScheme().equals(JDBCURI.JDBC_URL_SCHEME)) {
            throw new IllegalArgumentException("Not a relational jdbc url: " + uri);
        }
        // Pass the URI in and let the JDBCRelationalConnection try to
        // make sense of it and its parts (host, port, db, etc.) as it sees fit.
        return new JDBCRelationalConnection(uri, connectionOptions);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.toLowerCase(Locale.US).startsWith(JDBCURI.JDBC_BASE_URL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }
}
