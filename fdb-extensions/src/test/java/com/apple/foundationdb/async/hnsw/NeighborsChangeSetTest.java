/*
 * NeighborsChangeSetTest.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link NeighborsChangeSet#size()} and {@link NeighborsChangeSet#containsNeighbor(Tuple)} agree with what
 * {@link NeighborsChangeSet#merge()} yields. Both are established when a change set is constructed, from its parent
 * and its own delta, so a layered chain is where they can drift from the merged view.
 */
class NeighborsChangeSetTest {
    private static final int NUM_KEYS = 12;

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 987654321L, 42L})
    void sizeAndContainsAgreeWithMergeAcrossLayers(final long seed) {
        final Random random = new Random(seed);
        final List<Tuple> keys = new ArrayList<>(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; i++) {
            keys.add(Tuple.from(i));
        }

        // a base list of a random subset, then a chain of inserts and deletes of random keys on top of it
        NeighborsChangeSet<NodeReference> changeSet = new BaseNeighborsChangeSet<>(randomReferences(random, keys));
        assertAgreesWithMerge(changeSet, keys, "the base change set");

        for (int layer = 0; layer < 20; layer++) {
            final Tuple key = keys.get(random.nextInt(keys.size()));
            if (random.nextBoolean()) {
                changeSet = new InsertNeighborsChangeSet<>(changeSet, ImmutableList.of(new NodeReference(key)));
            } else {
                changeSet = new DeleteNeighborsChangeSet<>(changeSet, ImmutableList.of(key));
            }
            assertAgreesWithMerge(changeSet, keys, "layer " + layer);
        }
    }

    /**
     * Inserting a key the parent already holds replaces that entry rather than adding one, and deleting a key the
     * parent does not hold removes nothing. Both are the cases where a count maintained per layer can drift.
     */
    @Test
    void redundantInsertsAndDeletesDoNotChangeTheSize() {
        final Tuple held = Tuple.from(1);
        final Tuple notHeld = Tuple.from(2);
        final List<Tuple> keys = ImmutableList.of(held, notHeld);

        final NeighborsChangeSet<NodeReference> base =
                new BaseNeighborsChangeSet<>(ImmutableList.of(new NodeReference(held)));
        assertThat(base.size()).isEqualTo(1);

        final NeighborsChangeSet<NodeReference> redundantInsert =
                new InsertNeighborsChangeSet<>(base, ImmutableList.of(new NodeReference(held)));
        assertThat(redundantInsert.size()).as("inserting a key the parent holds must not raise the size").isEqualTo(1);
        assertAgreesWithMerge(redundantInsert, keys, "a redundant insert");

        final NeighborsChangeSet<NodeReference> redundantDelete =
                new DeleteNeighborsChangeSet<>(base, ImmutableList.of(notHeld));
        assertThat(redundantDelete.size()).as("deleting a key the parent does not hold must not lower the size")
                .isEqualTo(1);
        assertAgreesWithMerge(redundantDelete, keys, "a redundant delete");

        // deleting and then re-inserting the same key returns to the original size
        final NeighborsChangeSet<NodeReference> deletedThenInserted =
                new InsertNeighborsChangeSet<>(new DeleteNeighborsChangeSet<>(base, ImmutableList.of(held)),
                        ImmutableList.of(new NodeReference(held)));
        assertThat(deletedThenInserted.size()).isEqualTo(1);
        assertAgreesWithMerge(deletedThenInserted, keys, "a delete followed by the matching insert");
    }

    private static void assertAgreesWithMerge(@Nonnull final NeighborsChangeSet<NodeReference> changeSet,
                                              @Nonnull final List<Tuple> keys,
                                              @Nonnull final String where) {
        final Set<Tuple> merged = new LinkedHashSet<>();
        for (final NodeReference neighbor : changeSet.merge()) {
            assertThat(merged.add(neighbor.getPrimaryKey()))
                    .as("%s: merge() must not yield the same primary key twice", where)
                    .isTrue();
        }
        assertThat(changeSet.size())
                .as("%s: size() must equal the number of neighbors merge() yields", where)
                .isEqualTo(Iterables.size(changeSet.merge()))
                .isEqualTo(merged.size());
        for (final Tuple key : keys) {
            assertThat(changeSet.containsNeighbor(key))
                    .as("%s: containsNeighbor(%s) must match what merge() yields", where, key)
                    .isEqualTo(merged.contains(key));
        }
    }

    @Nonnull
    private static List<NodeReference> randomReferences(@Nonnull final Random random,
                                                       @Nonnull final List<Tuple> keys) {
        final ImmutableList.Builder<NodeReference> referencesBuilder = ImmutableList.builder();
        for (final Tuple key : keys) {
            if (random.nextBoolean()) {
                referencesBuilder.add(new NodeReference(key));
            }
        }
        return referencesBuilder.build();
    }
}
