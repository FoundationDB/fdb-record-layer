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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Warn: this class is stateful.
 * TODO (Make prepared statement parameters stateless)
 */
@API(API.Status.EXPERIMENTAL)
public final class PreparedParams {

    @Nonnull
    private static final PreparedParams EMPTY_PARAMETERS = new PreparedParams(Map.of(), Map.of());

    @Nonnull
    private final Map<Integer, Object> unnamedParams;

    @Nonnull
    private final Map<String, Object> namedParams;

    private int nextParam = 1;

    private PreparedParams(@Nonnull Map<Integer, Object> unnamedParams,
                           @Nonnull Map<String, Object> namedParameters) {
        this.unnamedParams = unnamedParams;
        this.namedParams = namedParameters;
    }

    public int currentUnnamedParamIndex() {
        return nextParam;
    }

    @Nullable
    public Object nextUnnamedParamValue() {
        Assert.thatUnchecked(unnamedParams.containsKey(nextParam),
                ErrorCode.UNDEFINED_PARAMETER, "No value found for parameter " + nextParam
        );
        return unnamedParams.get(nextParam++);
    }

    @Nullable
    public Object namedParamValue(@Nonnull String name) {
        Assert.thatUnchecked(namedParams.containsKey(name),
                ErrorCode.UNDEFINED_PARAMETER, "No value found for parameter " + name
        );
        return namedParams.get(name);
    }

    public boolean isEmpty() {
        return this.namedParams.isEmpty() && this.unnamedParams.isEmpty();
    }

    @Nonnull
    public static PreparedParams empty() {
        return EMPTY_PARAMETERS;
    }

    @Nonnull
    public static PreparedParams of(@Nonnull Map<Integer, Object> parameters,
                                    @Nonnull Map<String, Object> namedParameters) {
        return new PreparedParams(parameters, namedParameters);
    }

    @Nonnull
    public static PreparedParams ofUnnamed(@Nonnull Map<Integer, Object> parameters) {
        return of(parameters, ImmutableMap.of());
    }

    @Nonnull
    public static PreparedParams ofNamed(@Nonnull Map<String, Object> parameters) {
        return new PreparedParams(ImmutableMap.of(), parameters);
    }

    @Nonnull
    public static PreparedParams copyOf(@Nonnull PreparedParams other) {
        return new PreparedParams(other.unnamedParams, other.namedParams);
    }

    @Nonnull
    public static String prettyPrintParam(@Nullable Object value) {
        try {
            if (value == null) {
                return "<<NULL>>";
            } else if (value instanceof Byte) {
                return new Formatter().format("%02x", value).toString();
            } else if (value.getClass().isArray()) {
                final var result = new StringBuilder("Array[");
                int length = java.lang.reflect.Array.getLength(value);
                for (int count = 0; count < length; count++ ) {
                    result.append(prettyPrintParam(java.lang.reflect.Array.get(value, count)));
                    if (count < length - 1) {
                        result.append(",");
                    }
                }
                return result.append("]").toString();
            } else if (value instanceof Struct) {
                final var struct = (Struct)value;
                return "Struct" + Arrays.stream(struct.getAttributes())
                        .map(PreparedParams::prettyPrintParam).collect(Collectors.joining(",", "{", "}"));
            } else if (value instanceof String) {
                return "'" + value + "'";
            }
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        return value.toString();
    }
}
