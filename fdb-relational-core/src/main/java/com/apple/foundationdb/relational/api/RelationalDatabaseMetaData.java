/*
 * RelationalDatabaseMetaData.java
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

import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;

/**
 * Represents the MetaData about a database (schemas, tables, indexes, and so on).
 */
public interface RelationalDatabaseMetaData {

    /**
     * Get the schemas for this database, as a list.
     *
     * The returned resultset has two fields:
     * <ol>
     *   <li> DB_PATH = the full path of the database</li>
     *   <li> SCHEMA_NAME = the name of the schema</li>
     * </ol>
     *
     * @return a resultset with a schema listing.
     * @throws RelationalException if something goes wrong
     */
    @Nonnull RelationalResultSet getSchemas() throws RelationalException;

    /**
     * Get the tables for the specified schema within this database, as a ResultSet.
     *
     * The returned resultset has the following fields:
     * <ol>
     *   <li> NAME = the name of the table</li>
     * </ol>
     *
     * @param schema the name of the schema to search.
     * @return a resultset with a schema listing.
     * @throws RelationalException with ErrorCode {@link ErrorCode#SCHEMA_NOT_FOUND} if the schema
     * does not exist within this database; a different error code if something systemic goes wrong.
     */
    @Nonnull RelationalResultSet getTables(@Nonnull String schema) throws RelationalException;

    /**
     * Get the columns for the specified table within the specified schema, as a ResultSet.
     *
     * The returned resultset has the following fields:
     * <ol>
     *  <li> NAME = the name of the column</li>
     *  <li> INDEX = the index of the column within the type</li>
     *  <li> TYPE = a string-formatted representation of the type of the column</li>
     *  <li> OPTIONS = a json-style string holding all the options set on that field by the descriptor.</li>
     * </ol>
     *
     * @param schema the schema of interest.
     * @param table the table of interest. There must be a table with this name in the specified schema, or a
     *              {@link ErrorCode#UNDEFINED_TABLE} will be thrown.
     * @return a ResultSet with a column listing for the table.
     * @throws RelationalException if something goes wrong.
     */
    //TODO(bfines) allow parsing the schema from the table using SQL dot notation
    @Nonnull RelationalResultSet getColumns(@Nonnull String schema, @Nonnull String table) throws RelationalException;

    /**
     * Get the TableMetaData for the table specified.
     *
     * @param schema the schema of interest.
     * @param table the table of interest. There must be a table with this name in the specified schema, or a
     *              {@link ErrorCode#UNDEFINED_TABLE} will be thrown.
     * @return the TableMetaData for the Table directly.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull TableMetaData describeTable(@Nonnull String schema, @Nonnull String table)throws RelationalException;

    /**
     * Get the path of the database this metadata is for.
     *
     * @return the full of the database this is for.
     */
    @Nonnull URI getDatabasePath();
}
