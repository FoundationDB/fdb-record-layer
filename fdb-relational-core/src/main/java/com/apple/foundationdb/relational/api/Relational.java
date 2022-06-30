/*
 * Relational.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Relational {
    private static volatile RelationalDriver registeredDriver;

    public static RelationalConnection connect(@Nonnull URI url, @Nonnull Options connectionOptions) throws RelationalException {
        return connect(url, null, connectionOptions);
    }

    public static RelationalConnection connect(@Nonnull URI url, @Nullable Transaction existingTransaction, @Nonnull Options connectionOptions) throws RelationalException {
        return connect(url, existingTransaction, TransactionConfig.DEFAULT, connectionOptions);
    }

    public static RelationalConnection connect(@Nonnull URI url, @Nullable Transaction existingTransaction, @Nonnull TransactionConfig transactionConfig, @Nonnull Options connectionOptions) throws RelationalException {
        // All connection URLs should start with "jdbc" so we strip that out from the URI and pass the remainder in
        String scheme = url.getScheme();
        if (scheme == null) {
            throw new RelationalException("Invalid connection url <" + url + ">. A valid url must follow this pattern: <scheme>:<driver>:/<db_name>, for example: jdbc:embed:/myDatabase", ErrorCode.INVALID_PATH);
        }
        if (!"jdbc".equalsIgnoreCase(scheme)) {
            throw new RelationalException("Unable to connect to url <" + url + ">: invalid scheme <" + scheme + ">. Supported schemes: [jdbc]", ErrorCode.INVALID_PATH);
        }
        URI nonSchemeUri = URI.create(url.toString().substring(5));
        return getDriver(nonSchemeUri).connect(nonSchemeUri, existingTransaction, transactionConfig, connectionOptions);
    }

    public static RelationalDriver getDriver(@Nonnull URI connectionUrl) throws RelationalException {
        if (registeredDriver != null && registeredDriver.acceptsURL(connectionUrl)) {
            return registeredDriver;
        }
        throw new RelationalException("No Driver registered which can interpret scheme <" + connectionUrl.getScheme() + ">. Supported drivers: [embed]", ErrorCode.UNABLE_TO_ESTABLISH_SQL_CONNECTION);
    }

    public static synchronized void registerDriver(@Nonnull RelationalDriver newDriver) throws RelationalException {
        if (registeredDriver != null) {
            throw new RelationalException("A driver is currently registered. Please deregister it before registering a new driver", ErrorCode.PROTOCOL_VIOLATION);
        }
        registeredDriver = newDriver;
    }

    public static synchronized void deregisterDriver(@Nonnull RelationalDriver driver) throws RelationalException {
        if (driver.equals(registeredDriver)) {
            registeredDriver = null;
        } else {
            throw new RelationalException("Attempted to deregister a driver that was not registered", ErrorCode.PROTOCOL_VIOLATION);
        }
    }

    private Relational() {
    }
}
