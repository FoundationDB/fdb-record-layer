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
import com.apple.foundationdb.record.planprotos.PVectorIndexEngineKind;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.collect.BiMap;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Objects;

/**
 * The kinds of vector engine, as selectable through the {@link IndexOptions#VECTOR_ENGINE} index option. Public so
 * consumers of the record layer can name the engine an index is (or should be) built with; the {@link VectorIndexEngine}
 * implementations themselves remain internal.
 */
@API(API.Status.EXPERIMENTAL)
public enum VectorIndexEngineKind {
    HNSW,
    GUARDIANN;

    @Nonnull
    private static final BiMap<VectorIndexEngineKind, PVectorIndexEngineKind> TO_PROTO =
            PlanSerialization.protoEnumBiMap(VectorIndexEngineKind.class, PVectorIndexEngineKind.class);

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

    /**
     * Converts this engine kind to its protobuf equivalent, so that a plan can carry it.
     *
     * @return the protobuf equivalent of this engine kind
     */
    @Nonnull
    public PVectorIndexEngineKind toProto() {
        return Objects.requireNonNull(TO_PROTO.get(this));
    }

    /**
     * Converts an engine kind back from its protobuf equivalent.
     *
     * @param vectorIndexEngineKindProto the protobuf engine kind
     * @return the engine kind
     */
    @Nonnull
    public static VectorIndexEngineKind fromProto(@Nonnull final PVectorIndexEngineKind vectorIndexEngineKindProto) {
        return Objects.requireNonNull(TO_PROTO.inverse().get(vectorIndexEngineKindProto));
    }
}
