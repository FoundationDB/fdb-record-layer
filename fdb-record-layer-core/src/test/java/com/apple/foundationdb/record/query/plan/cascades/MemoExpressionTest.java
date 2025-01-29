/*
 * MemoExpressionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the Memo data structure implemented by {@link Reference} and {@link RelationalExpression}.
 */
public class MemoExpressionTest {
    @Nonnull
    private static final Map<String, SyntheticPlannerExpression> leafExpressions = new HashMap<>();
    @Nonnull
    private static final Map<String, SyntheticPlannerExpression> middleExpressions = new HashMap<>();

    static {
        for (int i = 1; i <= 4; i++) {
            final String name = "leaf" + i;
            leafExpressions.put(name, new SyntheticPlannerExpression(name));
        }

        // Middle expressions with one child
        for (int childMemberCount = 1; childMemberCount <= 4; childMemberCount++) {
            Reference middleChildGroup = Reference.empty();
            for (int i = 1; i <= childMemberCount; i++) {
                middleChildGroup.insert(leafExpressions.get("leaf" + i));
            }
            final String name = "middle" + childMemberCount;
            middleExpressions.put(name, new SyntheticPlannerExpression(name, Collections.singletonList(middleChildGroup)));
        }
        // Middle expressions with two children
        for (int childSplitPosition = 1; childSplitPosition <= 3; childSplitPosition++) {
            Reference leftGroup = Reference.empty();
            Reference rightGroup = Reference.empty();
            for (int i = 1; i <= childSplitPosition; i++) {
                leftGroup.insert(leafExpressions.get("leaf" + i));
            }
            for (int i = childSplitPosition + 1; i <= 4; i++) {
                rightGroup.insert(leafExpressions.get("leaf" + i));
            }
            assertEquals(4, leftGroup.getMembers().size() + rightGroup.getMembers().size());
            final String name = "middle" + childSplitPosition + "-" + (4 - childSplitPosition);
            middleExpressions.put(name, new SyntheticPlannerExpression(name, ImmutableList.of(leftGroup, rightGroup)));
        }
    }

    @Test
    public void identicalSets() {
        Reference justALeaf1 = Reference.of(leafExpressions.get("leaf1"));
        Reference justALeaf2 = Reference.of(leafExpressions.get("leaf2"));
        assertTrue(justALeaf1.containsAllInMemo(justALeaf1, AliasMap.emptyMap()));
        assertFalse(justALeaf1.containsAllInMemo(justALeaf2, AliasMap.emptyMap()));

        Reference multipleLeaves1 = Reference.from(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        Reference multipleLeaves2 = Reference.from(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"));
        assertTrue(multipleLeaves1.containsAllInMemo(multipleLeaves1, AliasMap.emptyMap()));
        assertFalse(multipleLeaves1.containsAllInMemo(multipleLeaves2, AliasMap.emptyMap()));

        Reference complexExpression = Reference.from(middleExpressions.get("middle1-3"), middleExpressions.get("middle2"));
        assertTrue(complexExpression.containsAllInMemo(complexExpression, AliasMap.emptyMap()));
    }

    @Test
    public void flatSets() {
        Reference allLeaves = Reference.from(leafExpressions.values());
        Reference justALeaf = Reference.of(leafExpressions.get("leaf1"));
        assertTrue(allLeaves.containsAllInMemo(justALeaf, AliasMap.emptyMap()));
        assertFalse(justALeaf.containsAllInMemo(allLeaves, AliasMap.emptyMap()));

        Reference multipleLeaves = Reference.from(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        assertTrue(allLeaves.containsAllInMemo(multipleLeaves, AliasMap.emptyMap()));
        assertFalse(multipleLeaves.containsAllInMemo(allLeaves, AliasMap.emptyMap()));
    }

    @Test
    public void complexReferences() {
        SyntheticPlannerExpression root1 = new SyntheticPlannerExpression("root1",
                ImmutableList.of(Reference.from(middleExpressions.get("middle1"), middleExpressions.get("middle2")),
                        Reference.from(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"))));
        SyntheticPlannerExpression root2 = new SyntheticPlannerExpression("root2",
                ImmutableList.of(Reference.from(middleExpressions.get("middle1-3"), middleExpressions.get("middle2-2")),
                        Reference.from(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"))));
        SyntheticPlannerExpression root1copy = new SyntheticPlannerExpression("root1",
                    ImmutableList.of(Reference.of(leafExpressions.get("leaf3")),
                            Reference.of(leafExpressions.get("leaf4"))));
        Reference firstTwoRoots = Reference.from(root1, root2);
        Reference allRoots = Reference.from(root1, root2, root1copy);
        assertEquals(3, allRoots.getMembers().size());

        assertTrue(firstTwoRoots.containsAllInMemo(Reference.of(root1), AliasMap.emptyMap()));
        assertTrue(firstTwoRoots.containsAllInMemo(Reference.from(root1, root2), AliasMap.emptyMap()));
        assertTrue(allRoots.containsAllInMemo(firstTwoRoots, AliasMap.emptyMap()));
        assertFalse(firstTwoRoots.containsAllInMemo(Reference.of(root1copy), AliasMap.emptyMap()));
        assertFalse(firstTwoRoots.containsAllInMemo(allRoots, AliasMap.emptyMap()));

        SyntheticPlannerExpression singleRefExpression = new SyntheticPlannerExpression("root1",
                ImmutableList.of(Reference.of(middleExpressions.get("middle1")), // has only a single member in its child group
                        Reference.of(leafExpressions.get("leaf1"))));
        assertTrue(firstTwoRoots.containsAllInMemo(Reference.of(singleRefExpression), AliasMap.emptyMap()));
    }

    @ParameterizedTest
    @ValueSource(longs = { 2L, 3L, 5L })
    public void memoInsertionAtRoot(long seed) {
        Set<SyntheticPlannerExpression> trackingSet = new HashSet<>();
        Reference reference = Reference.empty();
        Reference sample = Reference.empty(); // will contain every 5th expression

        final Random random = new Random(seed);
        for (int i = 0; i < 100; i++) {
            // Generate a random expression and insert it at the root.
            SyntheticPlannerExpression expression = SyntheticPlannerExpression.generate(random, 5);
            trackingSet.add(expression);
            reference.insert(expression);
            assertTrue(reference.containsInMemo(expression));
            if (i % 5 == 0) {
                sample.insert(expression);
                assertTrue(sample.containsInMemo(expression));
            }
        }

        for (SyntheticPlannerExpression expression : trackingSet) {
            assertTrue(reference.containsInMemo(expression));
        }
        assertTrue(reference.containsAllInMemo(sample, AliasMap.emptyMap()));
    }

    /**
     * A mock planner expression with very general semantics to test the correctness of various operations on the memo
     * data structure.
     */
    private static class SyntheticPlannerExpression implements RelationalExpression {
        @Nonnull
        private final String identity;
        @Nonnull
        private final List<Quantifier.ForEach> quantifiers;

        public SyntheticPlannerExpression(@Nonnull String identity) {
            this(identity, Collections.emptyList());
        }

        public SyntheticPlannerExpression(@Nonnull String identity,
                                          @Nonnull List<Reference> children) {
            this.identity = identity;
            this.quantifiers = Quantifiers.forEachQuantifiers(children);
        }

        @Nonnull
        @Override
        public List<? extends Quantifier> getQuantifiers() {
            return quantifiers;
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                             @Nonnull final AliasMap equivalencesMap) {
            if (this == otherExpression) {
                return true;
            }
            if (getClass() != otherExpression.getClass()) {
                return false;
            }
            return identity.equals(((SyntheticPlannerExpression)otherExpression).identity);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(final Object other) {
            return semanticEquals(other);
        }

        @Override
        public int hashCode() {
            return semanticHashCode();
        }

        @Override
        public int hashCodeWithoutChildren() {
            return Objects.hash(identity);
        }

        @Nonnull
        public static SyntheticPlannerExpression generate(@Nonnull Random random, int maxDepth) {
            String name = Integer.toString(random.nextInt());
            if (maxDepth == 0) {
                return new SyntheticPlannerExpression(name);
            }
            int numChildren = random.nextInt(4); // Uniform random integer 0 and 3 (inclusive)
            List<Reference> children = new ArrayList<>(numChildren);
            for (int i = 0; i < numChildren; i++) {
                children.add(Reference.of(generate(random, maxDepth - 1)));
            }
            return new SyntheticPlannerExpression(name, children);
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public SyntheticPlannerExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                final boolean shouldSimplifyValues,
                                                                @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
            return this;
        }

        @Nonnull
        @Override
        public Value getResultValue() {
            return new QueriedValue();
        }
    }
}
