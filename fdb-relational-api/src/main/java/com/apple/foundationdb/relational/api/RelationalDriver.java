/*
 * RelationalDriver.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.BuildVersion;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A Driver which is used to connect to a Relational Database.
 */
public interface RelationalDriver extends Driver {

    default RelationalConnection connect(@Nonnull URI url) throws SQLException {
        return connect(url, Options.NONE);
    }

    RelationalConnection connect(@Nonnull URI url, @Nonnull Options connectionOptions) throws SQLException;

    @Override
    default int getMajorVersion() {
        try {
            return BuildVersion.getInstance().getMajorVersion();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Override
    default int getMinorVersion() {
        try {
            return BuildVersion.getInstance().getMinorVersion();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Override
    default DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not Implemented in the Relational layer");
    }

    @Override
    default boolean jdbcCompliant() {
        return false;
    }

    @Override
    default Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Not Implemented in the Relational layer");
    }
}
