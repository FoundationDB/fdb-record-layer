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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.plan.temp.TopologicalSort.TopologicalOrderPermutationIterable;
import com.apple.foundationdb.record.query.plan.temp.TopologicalSort.TopologicalOrderPermutationIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
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
 * Tests of the Memo data structure implemented by {@link GroupExpressionRef} and {@link RelationalExpression}.
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
            GroupExpressionRef<SyntheticPlannerExpression> middleChildGroup = new GroupExpressionRef<>();
            for (int i = 1; i <= childMemberCount; i++) {
                middleChildGroup.insert(leafExpressions.get("leaf" + i));
            }
            final String name = "middle" + childMemberCount;
            middleExpressions.put(name, new SyntheticPlannerExpression(name, Collections.singletonList(middleChildGroup)));
        }
        // Middle expressions with two children
        for (int childSplitPosition = 1; childSplitPosition <= 3; childSplitPosition++) {
            GroupExpressionRef<SyntheticPlannerExpression> leftGroup = new GroupExpressionRef<>();
            GroupExpressionRef<SyntheticPlannerExpression> rightGroup = new GroupExpressionRef<>();
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
        GroupExpressionRef<SyntheticPlannerExpression> justALeaf1 = GroupExpressionRef.of(leafExpressions.get("leaf1"));
        GroupExpressionRef<SyntheticPlannerExpression> justALeaf2 = GroupExpressionRef.of(leafExpressions.get("leaf2"));
        assertTrue(justALeaf1.containsAllInMemo(justALeaf1, AliasMap.empty()));
        assertFalse(justALeaf1.containsAllInMemo(justALeaf2, AliasMap.empty()));

        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves1 = GroupExpressionRef.of(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves2 = GroupExpressionRef.of(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"));
        assertTrue(multipleLeaves1.containsAllInMemo(multipleLeaves1, AliasMap.empty()));
        assertFalse(multipleLeaves1.containsAllInMemo(multipleLeaves2, AliasMap.empty()));

        GroupExpressionRef<SyntheticPlannerExpression> complexExpression = GroupExpressionRef.of(middleExpressions.get("middle1-3"), middleExpressions.get("middle2"));
        assertTrue(complexExpression.containsAllInMemo(complexExpression, AliasMap.empty()));
    }

    @Test
    public void flatSets() {
        GroupExpressionRef<SyntheticPlannerExpression> allLeaves = GroupExpressionRef.from(leafExpressions.values());
        GroupExpressionRef<SyntheticPlannerExpression> justALeaf = GroupExpressionRef.of(leafExpressions.get("leaf1"));
        assertTrue(allLeaves.containsAllInMemo(justALeaf, AliasMap.empty()));
        assertFalse(justALeaf.containsAllInMemo(allLeaves, AliasMap.empty()));

        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves = GroupExpressionRef.of(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        assertTrue(allLeaves.containsAllInMemo(multipleLeaves, AliasMap.empty()));
        assertFalse(multipleLeaves.containsAllInMemo(allLeaves, AliasMap.empty()));
    }

    @Test
    public void complexGroupExpressions() {
        SyntheticPlannerExpression root1 = new SyntheticPlannerExpression("root1",
                ImmutableList.of(GroupExpressionRef.of(middleExpressions.get("middle1"), middleExpressions.get("middle2")),
                        GroupExpressionRef.of(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"))));
        SyntheticPlannerExpression root2 = new SyntheticPlannerExpression("root2",
                ImmutableList.of(GroupExpressionRef.of(middleExpressions.get("middle1-3"), middleExpressions.get("middle2-2")),
                        GroupExpressionRef.of(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"))));
        SyntheticPlannerExpression root1copy = new SyntheticPlannerExpression("root1",
                    ImmutableList.of(GroupExpressionRef.of(leafExpressions.get("leaf3")),
                            GroupExpressionRef.of(leafExpressions.get("leaf4"))));
        GroupExpressionRef<SyntheticPlannerExpression> firstTwoRoots = GroupExpressionRef.of(root1, root2);
        GroupExpressionRef<SyntheticPlannerExpression> allRoots = GroupExpressionRef.of(root1, root2, root1copy);
        assertEquals(3, allRoots.getMembers().size());

        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1), AliasMap.empty()));
        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1, root2), AliasMap.empty()));
        assertTrue(allRoots.containsAllInMemo(firstTwoRoots, AliasMap.empty()));
        assertFalse(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1copy), AliasMap.empty()));
        assertFalse(firstTwoRoots.containsAllInMemo(allRoots, AliasMap.empty()));

        SyntheticPlannerExpression singleRefExpression = new SyntheticPlannerExpression("root1",
                ImmutableList.of(GroupExpressionRef.of(middleExpressions.get("middle1")), // has only a single member in its child group
                        GroupExpressionRef.of(leafExpressions.get("leaf1"))));
        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(singleRefExpression), AliasMap.empty()));
    }

    @ParameterizedTest
    @ValueSource(longs = { 2L, 3L, 5L })
    public void memoInsertionAtRoot(long seed) {
        Set<SyntheticPlannerExpression> trackingSet = new HashSet<>();
        GroupExpressionRef<SyntheticPlannerExpression> groupExpression = new GroupExpressionRef<>();
        GroupExpressionRef<SyntheticPlannerExpression> sample = new GroupExpressionRef<>(); // will contain every 5th expression

        final Random random = new Random(seed);
        for (int i = 0; i < 100; i++) {
            // Generate a random expression and it insert it at the root.
            SyntheticPlannerExpression expression = SyntheticPlannerExpression.generate(random, 5);
            trackingSet.add(expression);
            groupExpression.insert(expression);
            assertTrue(groupExpression.containsInMemo(expression));
            if (i % 5 == 0) {
                sample.insert(expression);
                assertTrue(sample.containsInMemo(expression));
            }
        }

        for (SyntheticPlannerExpression expression : trackingSet) {
            assertTrue(groupExpression.containsInMemo(expression));
        }
        assertTrue(groupExpression.containsAllInMemo(sample, AliasMap.empty()));
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
                                          @Nonnull List<ExpressionRef<SyntheticPlannerExpression>> children) {
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
            if (!RelationalExpression.super.equalsWithoutChildren(otherExpression, equivalencesMap)) {
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
            List<ExpressionRef<SyntheticPlannerExpression>> children = new ArrayList<>(numChildren);
            for (int i = 0; i < numChildren; i++) {
                children.add(GroupExpressionRef.of(generate(random, maxDepth - 1)));
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
        public SyntheticPlannerExpression rebase(@Nonnull final AliasMap translationMap) {
            return this;
        }
    }

    @Test
    public void testTopologicalSortImpossibleDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b), a, ImmutableSet.of(c));

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopologicalSortFullDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b));

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(1, topologicalPermutations.size());

        assertEquals(ImmutableList.of(a, b, c), topologicalPermutations.get(0));
    }

    @Test
    public void testTopologicalSortNoDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(6, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, c, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a)));
    }

    @Test
    public void testTopologicalSortSomeDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(c, ImmutableSet.of(a));

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(3, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
    }
    
    @Test
    public void testTopologicalSortSkip() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator = topologicalPermutationIterable.iterator();
        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();

        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();

            builder.add(next);

            if (next.get(0).equals(b) && next.get(1).equals(a)) {
                iterator.skip(0);
            }
        }

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = builder.build();

        assertEquals(5, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a)));
    }

    @Test
    public void testTopologicalSortEmpty() {
        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(), id -> ImmutableSet.of());

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopologicalSortSingle() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a), id -> ImmutableSet.of());

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(1, topologicalPermutations.size());
    }

    @Test
    public void testTopologicalSortSkipEmptyError() {
        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(), id -> ImmutableSet.of());

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipSingleError() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a), id -> ImmutableSet.of());

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipComplexError1() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b), id -> ImmutableSet.of());

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipComplexError2() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");

        final TopologicalOrderPermutationIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b), id -> ImmutableSet.of());

        final TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> iterator.skip(2));
    }
}
