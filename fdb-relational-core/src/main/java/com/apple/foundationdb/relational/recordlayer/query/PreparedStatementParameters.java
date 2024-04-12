/*
 * ParserContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Warn: this class is stateful.
 * TODO (Make prepared statement parameters stateless)
 */
public final class PreparedStatementParameters {

    @Nonnull
    private static final PreparedStatementParameters EMPTY_PARAMETERS = new PreparedStatementParameters(Map.of(), Map.of());

    @Nonnull
    private Map<Integer, Object> parameters;

    @Nonnull
    private Map<String, Object> namedParameters;

    private int nextParam = 1;

    private PreparedStatementParameters(@Nonnull final Map<Integer, Object> parameters,
                                        @Nonnull final Map<String, Object> namedParameters) {
        this.parameters = parameters;
        this.namedParameters = namedParameters;
    }

    public int getCurrentParameterIndex() {
        return nextParam;
    }

    public Object getNextParameter() {
        Assert.thatUnchecked(parameters != null && parameters.containsKey(nextParam),
                ErrorCode.UNDEFINED_PARAMETER, "No value found for parameter " + nextParam
        );
        return parameters.get(nextParam++);
    }

    public Object getNamedParameter(String name) {
        Assert.thatUnchecked(namedParameters != null && namedParameters.containsKey(name),
                ErrorCode.UNDEFINED_PARAMETER, "No value found for parameter " + name
        );
        return namedParameters.get(name);
    }

    public boolean isEmpty() {
        return this.namedParameters.isEmpty() && this.parameters.isEmpty();
    }

    @Nonnull
    public static PreparedStatementParameters empty() {
        return EMPTY_PARAMETERS;
    }

    @Nonnull
    public static PreparedStatementParameters of(@Nonnull final Map<Integer, Object> parameters, @Nonnull final Map<String, Object> namedParameters) {
        return new PreparedStatementParameters(parameters, namedParameters);
    }

    @Nonnull
    public static PreparedStatementParameters ofUnnamed(@Nonnull final Map<Integer, Object> parameters) {
        return of(parameters, Map.of());
    }

    @Nonnull
    public static PreparedStatementParameters ofNamed(@Nonnull final Map<String, Object> parameters) {
        return new PreparedStatementParameters(Map.of(), parameters);
    }

    @Nonnull
    public static PreparedStatementParameters of(@Nonnull final PreparedStatementParameters other) {
        return new PreparedStatementParameters(other.parameters, other.namedParameters);
    }
}
