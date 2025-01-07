/*
 * LogicalQuery.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * An immutable representation of a Query.
 *
 * This is primarily used as a partial key in the plan cache, so it needs to be immutable _and_ a representation
 * of the logical structure of a plan.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalQuery {
    private final String query;
    private final long queryHash;

    //TODO(bfines) this is where we store the query literals
//    private final Set<Object> queryLiterals;


    public LogicalQuery(@Nonnull String query, long queryHash) {
        this.query = query;
        this.queryHash = queryHash;
    }

    public static LogicalQuery of(@Nonnull String query, @Nonnull RelationalExpression relExp) {
        return new LogicalQuery(query, relExp.semanticHashCode());
    }

    public String getQuery() {
        return query;
    }

    public long getQueryHash() {
        return queryHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogicalQuery)) {
            return false;
        }
        LogicalQuery that = (LogicalQuery) o;
        return queryHash == that.queryHash && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryHash);
    }
}
