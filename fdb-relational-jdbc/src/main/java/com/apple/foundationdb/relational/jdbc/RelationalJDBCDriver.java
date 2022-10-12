/*
 * RelationalJDBCDriver.java
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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
// Unavoidable. This is part of the JDBC Driver Interface... getParentLogger returns it.
// grpc uses JUL too.
import java.util.logging.Logger;

/**
 * Relational JDBC Driver.
 * Use mysql/postgres URL format; i.e. jdbc:relational://HOST[:PORT].
 */
@SuppressWarnings({"PMD.SystemPrintln"}) // Used in extreme when a failure to register Driver
public class RelationalJDBCDriver implements Driver {
    // Load this driver and register it with the DriverManager.
    // From Code Example 9-1 in the JDBC 4.3 spec.
    static {
        try {
            DriverManager.registerDriver(new RelationalJDBCDriver());
        } catch (SQLException sqlException) {
            System.err.println(sqlException);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Not a relational jdbc url: " + url);
        }
        // Pass the 'url' string and let the RelationalJDBCConnection try to
        // make sense of it and its parts (host, port, db, etc.) as it sees fit.
        return new RelationalJDBCConnection(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Any url that starts with 'jdbc:relational://' is trying to be a relational jdbc url.
        // Accept it.
        return url.startsWith(JDBCConstants.JDBC_URL_PREFIX + JDBCConstants.JDBC_URL_SCHEME + "://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
