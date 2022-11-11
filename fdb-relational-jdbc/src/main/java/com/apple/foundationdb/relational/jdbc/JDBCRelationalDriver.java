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

import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

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
 * JDBC Relational Driver.
 * Use mysql/postgres URL format; i.e. jdbc:relational://HOST[:PORT].
 */
@SuppressWarnings({"PMD.SystemPrintln"}) // Used in extreme when a failure to register Driver
public class JDBCRelationalDriver implements Driver {
    /**
     * Base URL.
     * {@link #acceptsURL(String)} accepts any jdbc url that starts with the below.
     * Used to check passed urls in {@link #acceptsURL(String)} but also as a key
     * to pick out the loaded driver from DriverManager.
     * jdbc:relational://...
     */
    static final String JDBC_BASE_URL = JDBCConstants.JDBC_URL_PREFIX + JDBCConstants.JDBC_URL_SCHEME + "://";
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
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Not a relational JDBC url: " + url);
        }
        // Pass the 'url' string and let the JDBCRelationalConnection try to
        // make sense of it and its parts (host, port, db, etc.) as it sees fit.
        return new JDBCRelationalConnection(url);
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
        return BuildVersion.getInstance().getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return BuildVersion.getInstance().getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Not implemented");
    }
}
