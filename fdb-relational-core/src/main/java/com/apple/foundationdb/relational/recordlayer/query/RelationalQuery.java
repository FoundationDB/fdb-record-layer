/*
 * RelationalQuery.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Queryable;
import com.apple.foundationdb.relational.api.WhereClause;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A simple Query structure for building custom queries inside the Query engine.
 */
public class RelationalQuery implements Queryable {
    private final String table;
    private final String schema;

    private final List<String> columns;
    private final WhereClause whereClause;
    private final boolean isExplain;
    private final QueryProperties queryOpts;

    public RelationalQuery(@Nonnull String table,
                         @Nullable String schema,
                         @Nullable List<String> columns,
                         @Nullable WhereClause whereClause,
                         boolean isExplain,
                         @Nullable QueryProperties queryOpts) {
        this.table = table;
        this.schema = schema;
        this.columns = columns == null ? Collections.emptyList() : columns;
        this.whereClause = whereClause;
        this.isExplain = isExplain;
        this.queryOpts = queryOpts == null ? QueryProperties.DEFAULT : queryOpts;
    }

    @Override
    @Nonnull
    public String getTable() {
        return table;
    }

    @Override
    @Nullable
    public String getSchema() {
        return schema;
    }

    @Override
    @Nullable
    public List<String> getColumns() throws RelationalException {
        return columns;
    }

    @Override
    @Nullable
    public WhereClause getWhereClause() {
        return whereClause;
    }

    @Override
    public boolean isExplain() {
        return isExplain;
    }

    @Override
    @Nonnull
    public QueryProperties getQueryOptions() {
        return queryOpts;
    }
}
