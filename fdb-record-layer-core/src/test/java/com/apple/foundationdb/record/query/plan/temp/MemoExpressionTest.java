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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

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
        assertTrue(justALeaf1.containsAllInMemo(justALeaf1));
        assertFalse(justALeaf1.containsAllInMemo(justALeaf2));

        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves1 = GroupExpressionRef.of(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves2 = GroupExpressionRef.of(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"));
        assertTrue(multipleLeaves1.containsAllInMemo(multipleLeaves1));
        assertFalse(multipleLeaves1.containsAllInMemo(multipleLeaves2));

        GroupExpressionRef<SyntheticPlannerExpression> complexExpression = GroupExpressionRef.of(middleExpressions.get("middle1-3"), middleExpressions.get("middle2"));
        assertTrue(complexExpression.containsAllInMemo(complexExpression));
    }

    @Test
    public void flatSets() {
        GroupExpressionRef<SyntheticPlannerExpression> allLeaves = GroupExpressionRef.from(leafExpressions.values());
        GroupExpressionRef<SyntheticPlannerExpression> justALeaf = GroupExpressionRef.of(leafExpressions.get("leaf1"));
        assertTrue(allLeaves.containsAllInMemo(justALeaf));
        assertFalse(justALeaf.containsAllInMemo(allLeaves));

        GroupExpressionRef<SyntheticPlannerExpression> multipleLeaves = GroupExpressionRef.of(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        assertTrue(allLeaves.containsAllInMemo(multipleLeaves));
        assertFalse(multipleLeaves.containsAllInMemo(allLeaves));
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

        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1)));
        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1, root2)));
        assertTrue(allRoots.containsAllInMemo(firstTwoRoots));
        assertFalse(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(root1copy)));
        assertFalse(firstTwoRoots.containsAllInMemo(allRoots));

        SyntheticPlannerExpression singleRefExpression = new SyntheticPlannerExpression("root1",
                ImmutableList.of(GroupExpressionRef.of(middleExpressions.get("middle1")), // has only a single member in its child group
                        GroupExpressionRef.of(leafExpressions.get("leaf1"))));
        assertTrue(firstTwoRoots.containsAllInMemo(GroupExpressionRef.of(singleRefExpression)));
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
        assertTrue(groupExpression.containsAllInMemo(sample));
    }

    /**
     * A mock planner expression with very general semantics to test the correctness of various operations on the memo
     * data structure.
     */
    private static class SyntheticPlannerExpression implements RelationalExpression {
        @Nonnull
        private final String identity;
        @Nonnull
        private final List<ExpressionRef<SyntheticPlannerExpression>> childRefs;

        public SyntheticPlannerExpression(@Nonnull String identity) {
            this(identity, Collections.emptyList());
        }

        public SyntheticPlannerExpression(@Nonnull String identity,
                                          @Nonnull List<ExpressionRef<SyntheticPlannerExpression>> children) {
            this.identity = identity;
            this.childRefs = children;
        }

        @Nonnull
        @Override
        public Iterator<? extends ExpressionRef<? extends RelationalExpression>> getPlannerExpressionChildren() {
            return childRefs.iterator();
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
            if (!(otherExpression instanceof SyntheticPlannerExpression)) {
                return false;
            }
            return identity.equals(((SyntheticPlannerExpression)otherExpression).identity);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SyntheticPlannerExpression that = (SyntheticPlannerExpression)o;
            if (!Objects.equals(identity, that.identity) ||
                    childRefs.size() != that.childRefs.size()) {
                return false;
            }
            for (int i = 0; i < childRefs.size(); i++) {
                if (!childRefs.get(i).get().equals(that.childRefs.get(i).get())) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity, childRefs.stream().map(ExpressionRef::get).collect(Collectors.toList()));
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
    }
}
