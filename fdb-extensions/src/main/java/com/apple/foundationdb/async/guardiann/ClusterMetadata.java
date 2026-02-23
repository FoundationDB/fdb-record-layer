/*
 * ClusterMetadata.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.guardiann;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

class ClusterMetadata {
    @Nonnull
    private final UUID id;
    private final int numVectors;
    @Nonnull
    private final State state;

    public ClusterMetadata(@Nonnull final UUID id, final int numVectors, final int stateCode) {
        this(id, numVectors, State.ofCode(stateCode));
    }

    public ClusterMetadata(@Nonnull final UUID id, final int numVectors, @Nonnull final State state) {
        this.id = id;
        this.numVectors = numVectors;
        this.state = state;
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public int getNumVectors() {
        return numVectors;
    }

    @Nonnull
    public State getState() {
        return state;
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectors(@Nonnull final State state, final int numVectorsAdded) {
        return new ClusterMetadata(getId(), getNumVectors() + numVectorsAdded, state);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadata cluster = (ClusterMetadata)o;
        return Objects.equals(getId(), cluster.getId()) &&
                getNumVectors() == cluster.getNumVectors() &&
                getState() == cluster.getState();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getNumVectors(), getState().name());
    }

    public enum State {
        ACTIVE(0),
        SPLIT_MERGE(1);

        private static final Map<Integer, State> BY_CODE =
                Arrays.stream(values())
                        .collect(Collectors.toMap(s -> s.code, s -> s));

        private final int code;

        State(final int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static State ofCode(final int code) {
            return Objects.requireNonNull(BY_CODE.getOrDefault(code, null));
        }
    }
}
