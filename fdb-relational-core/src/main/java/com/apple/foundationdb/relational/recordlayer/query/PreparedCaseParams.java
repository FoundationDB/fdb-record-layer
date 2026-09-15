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
 * Turns one prepared case of a stored query into the {@link PreparedParams} that warm-up plans it with, so a warmed plan
 * is built through the path a client's prepared statement takes. {@code IS_NULL}, {@code IS_TRUE} and {@code IS_FALSE}
 * bind a real value; {@code IS_NOT_NULL} binds nothing and is planned from its declaration instead.
 *
 * <p>Every declaration is passed on, not just the ones a case leaves value-free, because a declaration is only read for
 * a parameter that carries no value — so there is nothing to filter per case.</p>
 */
@API(API.Status.EXPERIMENTAL)
final class PreparedCaseParams {

    private PreparedCaseParams() {
    }

    /**
     * Builds the parameters for one case.
     *
     * @param declarations the parameter list, from parameter name to the SQL text of its declaration
     * @param preparedCase the case, from parameter name to the state it is pinned to
     * @return the parameters to plan this case with
     */
    @Nonnull
    static PreparedParams of(@Nonnull final Map<String, String> declarations,
                             @Nonnull final Map<String, StoredQuery.ParameterState> preparedCase) {
        // A HashMap, since IS_NULL binds a real null and neither Map.of nor ImmutableMap accepts one. The distinction
        // matters: hasNamedParamValue asks containsKey, so bound-to-null is value-bound while absent is value-free.
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
                    // Left without a value on purpose. Checked all the same, so that a state with no declaration
                    // behind it fails about the missing declaration rather than about a missing value.
                    Assert.thatUnchecked(declarations.containsKey(parameterName), ErrorCode.INTERNAL_ERROR,
                            () -> "prepared case names '" + parameterName + "', which the parameter list does not declare");
                    break;
                default:
                    throw Assert.failUnchecked(ErrorCode.INTERNAL_ERROR,
                            "unhandled prepared case state " + entry.getValue());
            }
        }
        return PreparedParams.ofNamed(values).withDeclarations(declarations);
    }
}
