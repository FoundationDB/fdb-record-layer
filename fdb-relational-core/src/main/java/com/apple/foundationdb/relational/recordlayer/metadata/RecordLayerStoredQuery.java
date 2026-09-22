/*
 * RecordLayerStoredQuery.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.metadata.StoredQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * A {@link StoredQuery} in the Record Layer metadata system.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordLayerStoredQuery implements StoredQuery {
    @Nonnull
    private final String name;
    @Nonnull
    private final String query;
    @Nonnull
    private final List<String> tempFunctions;
    @Nonnull
    private final Map<String, String> parameters;
    @Nonnull
    private final List<Map<String, ParameterState>> preparedCases;

    public RecordLayerStoredQuery(@Nonnull final String name, @Nonnull final String query,
                                  @Nonnull final List<String> tempFunctions) {
        this(name, query, tempFunctions, ImmutableMap.of(), ImmutableList.of());
    }

    public RecordLayerStoredQuery(@Nonnull final String name, @Nonnull final String query,
                                  @Nonnull final List<String> tempFunctions,
                                  @Nonnull final Map<String, String> parameters,
                                  @Nonnull final List<Map<String, ParameterState>> preparedCases) {
        this.name = name;
        this.query = query;
        this.tempFunctions = ImmutableList.copyOf(tempFunctions);
        // ImmutableMap, not Map.copyOf: the latter randomizes iteration order per JVM run, which would serialize the
        // same metadata to different bytes.
        this.parameters = ImmutableMap.copyOf(parameters);
        this.preparedCases = preparedCases.stream()
                .map(ImmutableMap::copyOf)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public String getQuery() {
        return query;
    }

    @Nonnull
    @Override
    public List<String> getTempFunctions() {
        return tempFunctions;
    }

    @Nonnull
    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Nonnull
    @Override
    public List<Map<String, ParameterState>> getPreparedCases() {
        return preparedCases;
    }
}
