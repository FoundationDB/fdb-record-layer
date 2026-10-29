/*
 * StoredQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * A SELECT query persisted on a {@link SchemaTemplate}, paired with the
 * {@code CREATE [OR REPLACE]? TEMPORARY FUNCTION ...} declarations that must be installed
 * before the SELECT is planned.
 *
 * <p>The SELECT body and each temp-function declaration are kept as their original verbatim source.</p>
 */
public interface StoredQuery extends Metadata {
    /**
     * How one parameter is pinned in a prepared case. These four are the only states that change the plan itself rather
     * than only its constraints. The constant names are the canonical tokens the wire format carries.
     */
    enum ParameterState {
        /** Warmed with a real null bound. */
        IS_NULL,
        /** Warmed value-free, with the declared type forced non-nullable. */
        IS_NOT_NULL,
        /** Warmed with {@code true} bound. */
        IS_TRUE,
        /** Warmed with {@code false} bound. */
        IS_FALSE
    }

    /**
     * The SELECT body, as its original verbatim source.
     * @return the query text.
     */
    @Nonnull
    String getQuery();

    /**
     * The {@code CREATE TEMPORARY FUNCTION} declarations to install before the body is planned.
     * @return the declaration texts, in the order written.
     */
    @Nonnull
    List<String> getTempFunctions();

    /**
     * The parameters this query declares, as a map from parameter name to the SQL text of its declaration: a type,
     * optionally followed by a nullability clause, exactly as written. Empty if the query declares no parameters.
     * @return the declared parameters, keyed by name.
     */
    @Nonnull
    Map<String, String> getParameters();

    /**
     * The combinations this query is warmed for, one plan each. Every case here gives every declared parameter a
     * state: one the author left out of the SQL is filled in before it reaches this point. Empty exactly when the query
     * declares no parameters.
     * @return one map per case, from parameter name to the state it is pinned to.
     */
    @Nonnull
    List<Map<String, ParameterState>> getPreparedCases();

    @Override
    default void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);
    }
}
