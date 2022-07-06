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
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Driver which is used to connect to a Relational Database.
 */
@ExcludeFromJacocoGeneratedReport
public interface RelationalDriver {
    default RelationalConnection connect(@Nonnull URI url) throws RelationalException {
        return connect(url, null, Options.NONE);
    }

    default RelationalConnection connect(@Nonnull URI url, @Nonnull Options connectionOptions) throws RelationalException {
        return connect(url, null, connectionOptions);
    }

    /**
     * Connect to the database that is located at the specified url.
     *
     * @param url                 the url path for the database structure.
     * @param existingTransaction the existing Transaction for this connection to reuse
     * @param connectionOptions   connection options that can be used to configure the connection
     * @return a connection to the specified database
     * @throws RelationalException if something goes wrong during opening the database (for example, if no
     *                           database can be found in the catalog for the specified database url)
     */
    RelationalConnection connect(@Nonnull URI url, @Nullable Transaction existingTransaction, @Nonnull Options connectionOptions) throws RelationalException;

    /**
     * Determine if this driver can be used for the specified scheme string.
     *
     * @param url the URL to check.
     * @return true if this driver can interpret this scheme, {@code false} otherwise.
     */
    boolean acceptsURL(URI url);

}
