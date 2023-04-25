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
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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

    public Object getNextParameter() {
        Assert.thatUnchecked(parameters != null && parameters.containsKey(nextParam),
                "No value found for parameter " + nextParam,
                ErrorCode.UNDEFINED_PARAMETER);
        return parameters.get(nextParam++);
    }

    public Object getNamedParameter(String name) {
        Assert.thatUnchecked(namedParameters != null && namedParameters.containsKey(name),
                "No value found for parameter " + name,
                ErrorCode.UNDEFINED_PARAMETER);
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

    /**
     * Calculate the stable hash value of the binding parameters. The stable hash value should be the same across implementations
     * of {@link Map}, as it assumes nothing of the implementation or iteration order.
     * The hash value can be used to detect changes in the bound parameter values, so that the query can be validated to
     * be the same as another query (e.g. when given in a continuation).
     * @return the calculated stable hash value for the bound parameters
     */
    public int stableHash() {
        return hash("parameters", parameters) + hash("namedParameters", namedParameters);
    }

    private int hash(String title, Map<? extends Comparable<?>, Object> map) {
        int mapHash = 0;
        // Empty and null maps will produce the same hash value
        if (map != null) {
            // Sort the entries by key, to fix the iteration order
            Map<? extends Comparable<?>, Object> sorted = new TreeMap<>(map);

            for (Map.Entry<?, Object> entry: sorted.entrySet()) {
                mapHash = 31 * mapHash + Objects.hash(entry.getKey(), entry.getValue());
            }
        }
        return title.hashCode() + mapHash;
    }
}
