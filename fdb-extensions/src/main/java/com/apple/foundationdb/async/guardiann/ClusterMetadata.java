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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

class ClusterMetadata {
    @Nonnull
    private final UUID id;
    private final int numPrimaryVectors;
    private final int numPrimaryUnderreplicatedVectors;
    private final int numReplicatedVectors;
    @Nonnull
    private final EnumSet<State> states;

    public ClusterMetadata(@Nonnull final UUID id, final int numPrimaryVectors,
                           final int numPrimaryUnderreplicatedVectors, final int numReplicatedVectors,
                           final int stateCode) {
        this(id, numPrimaryVectors, numPrimaryUnderreplicatedVectors, numReplicatedVectors, State.ofCode(stateCode));
    }

    public ClusterMetadata(@Nonnull final UUID id, final int numPrimaryVectors,
                           final int numPrimaryUnderreplicatedVectors, final int numReplicatedVectors,
                           @Nonnull final EnumSet<State> states) {
        Preconditions.checkArgument(numPrimaryVectors >= numPrimaryUnderreplicatedVectors);
        this.id = id;
        this.numPrimaryVectors = numPrimaryVectors;
        this.numPrimaryUnderreplicatedVectors = numPrimaryUnderreplicatedVectors;
        this.numReplicatedVectors = numReplicatedVectors;
        this.states = states;
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public int getNumPrimaryVectors() {
        return numPrimaryVectors;
    }

    public int getNumPrimaryUnderreplicatedVectors() {
        return numPrimaryUnderreplicatedVectors;
    }

    public int getNumReplicatedVectors() {
        return numReplicatedVectors;
    }

    @Nonnull
    public EnumSet<State> getStates() {
        return states;
    }

    public int getStatesCode() {
        int result = 0;
        for (final State state : getStates()) {
            result |= state.getCode();
        }
        return result;
    }

    @Nonnull
    public ClusterMetadata withNewVectors(final int numPrimaryVectors,
                                          final int numPrimaryUnderreplicatedVectors,
                                          final int numReplicatedVectors,
                                          @Nonnull final EnumSet<State> states) {
        final EnumSet<State> newStates = EnumSet.copyOf(states);
        return new ClusterMetadata(getId(), numPrimaryVectors, numPrimaryUnderreplicatedVectors,
                numReplicatedVectors, newStates);
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectors(final int numPrimaryVectorsAdded,
                                                 final int numPrimaryUnderreplicatedVectorsAdded,
                                                 final int numReplicatedVectorsAdded) {
        return withAdditionalVectorsAndNewStates(numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                numReplicatedVectorsAdded);
    }

    @Nonnull
    public ClusterMetadata withNewStates(@Nonnull final State... additionalStates) {
        return withAdditionalVectorsAndNewStates(0, 0,
                0, additionalStates);
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectorsAndNewStates(final int numPrimaryVectorsAdded,
                                                             final int numPrimaryUnderreplicatedVectorsAdded,
                                                             final int numReplicatedVectorsAdded,
                                                             @Nonnull final State... additionalStates) {
        final EnumSet<State> newStates = EnumSet.copyOf(getStates());
        Collections.addAll(newStates, additionalStates);
        return withAdditionalVectorsAndNewStates(numPrimaryVectorsAdded, numPrimaryUnderreplicatedVectorsAdded,
                numReplicatedVectorsAdded, newStates);
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectorsAndNewStates(final int numPrimaryVectorsAdded,
                                                             final int numPrimaryUnderreplicatedVectorsAdded,
                                                             final int numReplicatedVectorsAdded,
                                                             @Nonnull final EnumSet<State> additionalStates) {
        final EnumSet<State> newStates = EnumSet.copyOf(getStates());
        newStates.addAll(additionalStates);
        return new ClusterMetadata(getId(), getNumPrimaryVectors() + numPrimaryVectorsAdded,
                getNumPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded,
                getNumReplicatedVectors() + numReplicatedVectorsAdded, newStates);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadata cluster = (ClusterMetadata)o;
        return Objects.equals(getId(), cluster.getId()) &&
                getNumPrimaryVectors() == cluster.getNumPrimaryVectors() &&
                getNumPrimaryUnderreplicatedVectors() == cluster.getNumPrimaryUnderreplicatedVectors() &&
                getNumReplicatedVectors() == cluster.getNumReplicatedVectors() &&
                getStates().equals(cluster.getStates());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getNumPrimaryVectors(), getNumPrimaryUnderreplicatedVectors(),
                getNumPrimaryUnderreplicatedVectors(), getStatesCode());
    }

    @Override
    public String toString() {
        return "CM[id=" + getId() +
                ", numPrimaryVectors=" + getNumPrimaryVectors() +
                ", numPrimaryUnderreplicatedVectors=" + getNumPrimaryUnderreplicatedVectors() +
                ", numReplicatedVectors=" + getNumReplicatedVectors() +
                ", states=" + getStates() +
                ']';
    }

    public enum State {
        SPLIT_MERGE(1),
        REASSIGN(2);

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

        public static EnumSet<State> ofCode(final int code) {
            final EnumSet<State> resultSet = EnumSet.noneOf(State.class);
            for (int i = 0; i < 32; i++) {
                final int bitValue = 1 << i;
                if ((code & bitValue) != 0) {
                    final State lookup = BY_CODE.getOrDefault(bitValue, null);
                    Objects.requireNonNull(lookup, "unable to look up state");
                    resultSet.add(lookup);
                }
            }
            return resultSet;
        }
    }
}
