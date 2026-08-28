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
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableMap;
import org.jspecify.annotations.Nullable;
import java.util.Map;

/**
 * Warn: this class is stateful.
 * TODO (Make prepared statement parameters stateless)
 */
@API(API.Status.EXPERIMENTAL)
public final class PreparedParams {

    private static final PreparedParams EMPTY_PARAMETERS = new PreparedParams(Map.of(), Map.of());

    private final Map<Integer, Object> unnamedParams;

    private final Map<String, Object> namedParams;

    private int nextParam = 1;

    private PreparedParams(Map<Integer, Object> unnamedParams,
                           Map<String, Object> namedParameters) {
        this.unnamedParams = unnamedParams;
        this.namedParams = namedParameters;
    }

    private PreparedParams(Map<Integer, Object> unnamedParams,
                           Map<String, Object> namedParameters,
                           int nextParam) {
        this.unnamedParams = unnamedParams;
        this.namedParams = namedParameters;
        this.nextParam = nextParam;
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
    public Object namedParamValue(String name) {
        Assert.thatUnchecked(namedParams.containsKey(name),
                ErrorCode.UNDEFINED_PARAMETER, "No value found for parameter " + name
        );
        return namedParams.get(name);
    }

    public boolean isEmpty() {
        return this.namedParams.isEmpty() && this.unnamedParams.isEmpty();
    }

    public static PreparedParams empty() {
        return EMPTY_PARAMETERS;
    }

    public static PreparedParams of(Map<Integer, Object> parameters,
                                    Map<String, Object> namedParameters) {
        return new PreparedParams(parameters, namedParameters);
    }

    public static PreparedParams ofUnnamed(Map<Integer, Object> parameters) {
        return of(parameters, ImmutableMap.of());
    }

    public static PreparedParams ofNamed(Map<String, Object> parameters) {
        return new PreparedParams(ImmutableMap.of(), parameters);
    }

    public static PreparedParams copyOf(PreparedParams other) {
        return copyOf(other, false);
    }

    public static PreparedParams copyOf(PreparedParams other, boolean withCurrentUnnamedParamIndex) {
        if (withCurrentUnnamedParamIndex) {
            return new PreparedParams(other.unnamedParams, other.namedParams, other.currentUnnamedParamIndex());
        } else {
            return new PreparedParams(other.unnamedParams, other.namedParams);
        }
    }
}
