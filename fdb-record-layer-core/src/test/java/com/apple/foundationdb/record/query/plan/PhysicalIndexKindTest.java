/*
 * PhysicalIndexKindTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of {@link PhysicalIndexKind#combine}, which decides the kind of a plan from the kinds of its parts.
 */
class PhysicalIndexKindTest {
    @Test
    void nothingToCombineIsUnknown() {
        assertEquals(PhysicalIndexKind.UNKNOWN, PhysicalIndexKind.combine());
        assertEquals(PhysicalIndexKind.UNKNOWN, PhysicalIndexKind.combine(ImmutableList.of()));
    }

    /**
     * {@link PhysicalIndexKind#UNKNOWN} is first so that it stays at ordinal zero as kinds are added.
     */
    @Test
    void unknownIsTheFirstKind() {
        assertEquals(0, PhysicalIndexKind.UNKNOWN.ordinal());
    }

    @ParameterizedTest
    @EnumSource(PhysicalIndexKind.class)
    void oneKindCombinesToItself(@Nonnull final PhysicalIndexKind kind) {
        assertEquals(kind, PhysicalIndexKind.combine(kind));
    }

    @ParameterizedTest
    @EnumSource(PhysicalIndexKind.class)
    void severalOfTheSameKindCombineToThatKind(@Nonnull final PhysicalIndexKind kind) {
        assertEquals(kind, PhysicalIndexKind.combine(kind, kind, kind));
    }

    @Test
    void differingKindsCombineToMixed() {
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.HNSW, PhysicalIndexKind.GUARDIANN));
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.BTREE, PhysicalIndexKind.BTREE, PhysicalIndexKind.LUCENE));
    }

    /**
     * {@link PhysicalIndexKind#PRIMARY} is not an identity: a plan that reaches an index in one branch and not in the
     * other is of two minds about how it reaches records, and saying it is purely the one kind would overstate what is
     * known.
     */
    @Test
    void primaryIsNotAnIdentity() {
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.PRIMARY, PhysicalIndexKind.BTREE));
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.BTREE, PhysicalIndexKind.PRIMARY));
    }

    /**
     * {@link PhysicalIndexKind#IN_MEMORY} is the one identity: a temporary table alongside an index scan does not make a
     * plan ambiguous about which persistent structure it is built around, so it drops out of the combination.
     */
    @Test
    void inMemoryDropsOutOfTheCombination() {
        assertEquals(PhysicalIndexKind.BTREE,
                PhysicalIndexKind.combine(PhysicalIndexKind.BTREE, PhysicalIndexKind.IN_MEMORY));
        assertEquals(PhysicalIndexKind.BTREE,
                PhysicalIndexKind.combine(PhysicalIndexKind.IN_MEMORY, PhysicalIndexKind.BTREE));
        assertEquals(PhysicalIndexKind.HNSW,
                PhysicalIndexKind.combine(PhysicalIndexKind.IN_MEMORY, PhysicalIndexKind.HNSW,
                        PhysicalIndexKind.IN_MEMORY));
    }

    /**
     * Dropping out is not the same as making everything agree: kinds that disagree are still mixed, whether or not an
     * in-memory structure is along for the ride.
     */
    @Test
    void inMemoryDoesNotRescueDisagreeingKinds() {
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.GUARDIANN, PhysicalIndexKind.HNSW,
                        PhysicalIndexKind.BTREE));
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.GUARDIANN, PhysicalIndexKind.HNSW,
                        PhysicalIndexKind.BTREE, PhysicalIndexKind.IN_MEMORY));
    }

    @Test
    void onlyInMemoryStaysInMemory() {
        assertEquals(PhysicalIndexKind.IN_MEMORY,
                PhysicalIndexKind.combine(PhysicalIndexKind.IN_MEMORY, PhysicalIndexKind.IN_MEMORY));
        assertEquals(PhysicalIndexKind.IN_MEMORY,
                PhysicalIndexKind.combine(PhysicalIndexKind.IN_MEMORY));
    }

    /**
     * Combining is not a set union: once mixed, always mixed, whatever it is mixed with.
     */
    @Test
    void mixedStaysMixed() {
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.MIXED, PhysicalIndexKind.MIXED));
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.MIXED, PhysicalIndexKind.BTREE));
        assertEquals(PhysicalIndexKind.MIXED,
                PhysicalIndexKind.combine(PhysicalIndexKind.HNSW, PhysicalIndexKind.MIXED));
    }
}
