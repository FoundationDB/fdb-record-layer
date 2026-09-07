/*
 * PreparedCaseParams.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.StoredQuery;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Turns one prepared case of a stored query into the {@link PreparedParams} that warm-up plans it with, so that a
 * warmed plan is built through the very path a client's prepared statement takes.
 *
 * <p>
 * A state either binds a value or leaves the parameter without one:
 * </p>
 * <ul>
 *     <li>{@code IS_NULL}, {@code IS_TRUE} and {@code IS_FALSE} bind a real value, so the planner sees it and can fold
 *     predicates away exactly as it would at run time.</li>
 *     <li>{@code IS_NOT_NULL} binds nothing. The parameter is planned from its declaration instead, which the planning
 *     path resolves into a non-nullable type — that is what keeps a null binding from matching the resulting plan.</li>
 * </ul>
 *
 * <p>
 * Every declaration is passed on unchanged, not just the ones a case leaves value-free, because a declaration is only
 * ever read for a parameter that carries no value: one that is also bound never consults it. So there is nothing to
 * filter per case, and this stays a pure function of a signature and a case — no database, no schema template, no
 * planner.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
final class PreparedCaseParams {

    private PreparedCaseParams() {
    }

    /**
     * Builds the parameters for one case.
     *
     * @param declarations the signature, from parameter name to the SQL text of its declaration
     * @param preparedCase the case, from parameter name to the state it is pinned to
     * @return the parameters to plan this case with
     */
    @Nonnull
    static PreparedParams of(@Nonnull final Map<String, String> declarations,
                             @Nonnull final Map<String, StoredQuery.ParameterState> preparedCase) {
        // A HashMap rather than Map.of or ImmutableMap: IS_NULL binds a real null, which neither of those accepts. The
        // distinction matters — PreparedParams.hasNamedParamValue asks containsKey, so a parameter bound to null is
        // value-bound and reaches constant folding, while one merely absent would be planned value-free instead.
        final var values = new HashMap<String, Object>();
        for (final var entry : preparedCase.entrySet()) {
            final var parameterName = entry.getKey();
            switch (entry.getValue()) {
                case IS_NULL:
                    values.put(parameterName, null);
                    break;
                case IS_TRUE:
                    values.put(parameterName, Boolean.TRUE);
                    break;
                case IS_FALSE:
                    values.put(parameterName, Boolean.FALSE);
                    break;
                case IS_NOT_NULL:
                    // Deliberately left without a value: planning resolves the declaration instead. Checked here all
                    // the same, because a state with no declaration behind it would otherwise be planned as an ordinary
                    // unbound parameter and fail with a message about a missing value rather than a missing declaration.
                    Assert.thatUnchecked(declarations.containsKey(parameterName), ErrorCode.INTERNAL_ERROR,
                            () -> "prepared case names '" + parameterName + "', which the signature does not declare");
                    break;
                default:
                    throw Assert.failUnchecked(ErrorCode.INTERNAL_ERROR,
                            "unhandled prepared case state " + entry.getValue());
            }
        }
        return PreparedParams.ofNamed(values).withDeclarations(declarations);
    }
}
