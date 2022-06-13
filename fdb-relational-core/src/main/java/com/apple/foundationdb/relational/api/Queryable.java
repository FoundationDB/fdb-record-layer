/*
 * Queryable.java
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

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract representation of a Query data structure.
 */
public interface Queryable {

    /**
     * Get the schema that this Queryable is running against.
     *
     * @return the schema to execute this query within.
     */
    @Nullable
    String getSchema();

    /**
     * Get the name of the table to run this query against.
     *
     * @return the name of the table to execute this query against.
     */
    @Nonnull
    String getTable();

    /**
     * Get the specific columns to fetch with this query.
     *
     * @return a list of the column names to fetch with this query, or {@code null} to return all columns in the
     * query.
     */
    @Nullable
    List<String> getColumns();

    /**
     * Get the where clause for this query. The Where clause is the abstract tree of predicates
     * defining the query system.
     *
     * @return the WhereClause for this query, or {@code null} if no where clause is to be applied.
     */
    @Nullable
    WhereClause getWhereClause();

    /**
     * Determine if this query should be fully executed, or only planned and generated.
     *
     * @return {@code true} if the query is to be planned only, and {@code false} if the query is to be fully executed.
     */
    boolean isExplain();
}
