/*
 * VectorIndexEngineKind.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * The kinds of vector engine, as selectable through the {@link IndexOptions#VECTOR_ENGINE} index option. Public so
 * consumers of the record layer can name the engine an index is (or should be) built with; the {@link VectorIndexEngine}
 * implementations themselves remain internal.
 */
@API(API.Status.EXPERIMENTAL)
public enum VectorIndexEngineKind {
    HNSW,
    GUARDIANN;

    /**
     * Resolves an engine kind from its option string, accepting any letter case. When {@code value} is
     * {@code null} (the option was not set) the engine defaults to {@link #HNSW}, so vector indexes created before
     * the engine option existed continue to use HNSW.
     *
     * @param value the raw option value, or {@code null} if unset
     * @return the resolved engine kind
     */
    @Nonnull
    public static VectorIndexEngineKind fromOptionValue(final String value) {
        if (value == null) {
            return HNSW;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "HNSW" -> HNSW;
            case "GUARDIANN" -> GUARDIANN;
            default -> throw new MetaDataException("unknown vector index engine", LogMessageKeys.VALUE, value);
        };
    }
}
