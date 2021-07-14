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

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A Driver which is used to connect to a Relational Database
 */
public interface RelationalDriver {
    /**
     * Connect to the database that is located at the specified url.
     *
     * @param url the url path for the database structure.
     * @param connectionOptions connection options that can be used to configure the connection
     * @return a connection to the specified database
     * @throws RelationalException if something goes wrong during opening the database (for example, if no
     * database can be found in the catalog for the specified database url)
     */
    DatabaseConnection connect(@Nonnull List<Object> url, @Nonnull Options connectionOptions) throws RelationalException;

    /**
     * @return the major version of the Relational Driver.
     */
    int getMajorVersion();

    /**
     * @return the minor version of the Relational Driver.
     */
    int getMinorVersion();

}
