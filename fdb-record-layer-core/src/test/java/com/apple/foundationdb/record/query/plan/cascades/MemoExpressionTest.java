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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.rules.FinalizeExpressionsRule;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
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
                middleChildGroup.insertFinalExpression(leafExpressions.get("leaf" + i));
            }
            final String name = "middle" + childMemberCount;
            middleExpressions.put(name, SyntheticPlannerExpression.ofRefs(name, middleChildGroup));
        }
        // Middle expressions with two children
        for (int childSplitPosition = 1; childSplitPosition <= 3; childSplitPosition++) {
            Reference leftGroup = Reference.empty();
            Reference rightGroup = Reference.empty();
            for (int i = 1; i <= childSplitPosition; i++) {
                leftGroup.insertFinalExpression(leafExpressions.get("leaf" + i));
            }
            for (int i = childSplitPosition + 1; i <= 4; i++) {
                rightGroup.insertFinalExpression(leafExpressions.get("leaf" + i));
            }
            assertEquals(4, leftGroup.getAllMemberExpressions().size() + rightGroup.getAllMemberExpressions().size());
            final String name = "middle" + childSplitPosition + "-" + (4 - childSplitPosition);
            middleExpressions.put(name, SyntheticPlannerExpression.ofRefs(name, leftGroup, rightGroup));
        }
    }

    @Nonnull
    private static Memoizer createMemoizer(@Nonnull Reference root) {
        CascadesRule<?> rule = new FinalizeExpressionsRule(); // doesn't matter for memoization
        return new CascadesRuleCall(PlannerPhase.REWRITING, new FakePlanContext(), rule, root, Traversal.withRoot(root), new ArrayDeque<>(), PlannerBindings.empty(), EvaluationContext.empty());
    }

    @Nonnull
    private static Reference ofExploratoryExpressions(@Nonnull RelationalExpression... expressions) {
        return Reference.ofExploratoryExpressions(PlannerStage.CANONICAL, List.of(expressions));
    }

    @Test
    public void identicalSets() {
        Reference justALeaf1 = Reference.initialOf(leafExpressions.get("leaf1"));
        Reference justALeaf2 = Reference.initialOf(leafExpressions.get("leaf2"));
        assertTrue(justALeaf1.containsAllInMemo(justALeaf1, AliasMap.emptyMap()));
        assertFalse(justALeaf1.containsAllInMemo(justALeaf2, AliasMap.emptyMap()));

        Reference multipleLeaves1 = Reference.initialOf(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        Reference multipleLeaves2 = Reference.initialOf(leafExpressions.get("leaf3"), leafExpressions.get("leaf4"));
        assertTrue(multipleLeaves1.containsAllInMemo(multipleLeaves1, AliasMap.emptyMap()));
        assertFalse(multipleLeaves1.containsAllInMemo(multipleLeaves2, AliasMap.emptyMap()));

        Reference complexExpression = Reference.initialOf(middleExpressions.get("middle1-3"), middleExpressions.get("middle2"));
        assertTrue(complexExpression.containsAllInMemo(complexExpression, AliasMap.emptyMap()));
    }

    @Test
    public void flatSets() {
        Reference allLeaves = Reference.initialOf(leafExpressions.values());
        Reference justALeaf = Reference.initialOf(leafExpressions.get("leaf1"));
        assertTrue(allLeaves.containsAllInMemo(justALeaf, AliasMap.emptyMap()));
        assertFalse(justALeaf.containsAllInMemo(allLeaves, AliasMap.emptyMap()));

        Reference multipleLeaves = Reference.initialOf(leafExpressions.get("leaf1"), leafExpressions.get("leaf2"));
        assertTrue(allLeaves.containsAllInMemo(multipleLeaves, AliasMap.emptyMap()));
        assertFalse(multipleLeaves.containsAllInMemo(allLeaves, AliasMap.emptyMap()));
    }

    @Test
    public void complexReferences() {
        SyntheticPlannerExpression root1 = SyntheticPlannerExpression.ofRefs("root1",
                Reference.initialOf(middleExpressions.get("middle1"), middleExpressions.get("middle2")),
                        Reference.initialOf(leafExpressions.get("leaf1"), leafExpressions.get("leaf2")));
        SyntheticPlannerExpression root2 = SyntheticPlannerExpression.ofRefs("root2",
                Reference.initialOf(middleExpressions.get("middle1-3"), middleExpressions.get("middle2-2")),
                        Reference.initialOf(leafExpressions.get("leaf3"), leafExpressions.get("leaf4")));
        SyntheticPlannerExpression root1copy = SyntheticPlannerExpression.ofRefs("root1",
                    Reference.initialOf(leafExpressions.get("leaf3")),
                            Reference.initialOf(leafExpressions.get("leaf4")));
        Reference firstTwoRoots = Reference.initialOf(root1, root2);
        Reference allRoots = Reference.initialOf(root1, root2, root1copy);
        assertEquals(3, allRoots.getAllMemberExpressions().size());

        assertTrue(firstTwoRoots.containsAllInMemo(Reference.initialOf(root1), AliasMap.emptyMap()));
        assertTrue(firstTwoRoots.containsAllInMemo(Reference.initialOf(root1, root2), AliasMap.emptyMap()));
        assertTrue(allRoots.containsAllInMemo(firstTwoRoots, AliasMap.emptyMap()));
        assertFalse(firstTwoRoots.containsAllInMemo(Reference.initialOf(root1copy), AliasMap.emptyMap()));
        assertFalse(firstTwoRoots.containsAllInMemo(allRoots, AliasMap.emptyMap()));

        SyntheticPlannerExpression singleRefExpression = SyntheticPlannerExpression.ofRefs("root1",
                Reference.initialOf(middleExpressions.get("middle1")), // has only a single member in its child group
                        Reference.initialOf(leafExpressions.get("leaf1")));
        assertTrue(firstTwoRoots.containsAllInMemo(Reference.initialOf(singleRefExpression), AliasMap.emptyMap()));
    }

    @Test
    void checkCorrelated() {
        SyntheticPlannerExpression expr1 = SyntheticPlannerExpression.withCorrelated("a", "x", "y");
        final Reference ref = Reference.initialOf(expr1);

        // Should match when the expression has the same correlations
        SyntheticPlannerExpression expr2 = SyntheticPlannerExpression.withCorrelated("a", "x", "y");
        final Reference refWith2 = Reference.initialOf(expr2);
        assertTrue(ref.containsAllInMemo(refWith2, AliasMap.emptyMap()));

        // Should fail when the correlations are different
        SyntheticPlannerExpression expr3 = SyntheticPlannerExpression.withCorrelated("a", "y", "z");
        final Reference refWith3 = Reference.initialOf(expr3);
        assertFalse(ref.containsAllInMemo(refWith3, AliasMap.emptyMap()));

        // Should succeed when given a translation map that aligns the correlated ids
        AliasMap totalMap = AliasMap.builder(2)
                .put(CorrelationIdentifier.of("x"), CorrelationIdentifier.of("y"))
                .put(CorrelationIdentifier.of("y"), CorrelationIdentifier.of("z"))
                .build();
        assertTrue(ref.containsAllInMemo(refWith3, totalMap));
        AliasMap mapXToZ = AliasMap.ofAliases(CorrelationIdentifier.of("x"), CorrelationIdentifier.of("z"));
        assertTrue(ref.containsAllInMemo(refWith3, mapXToZ));

        // Do not succeed when the translation map does not equate the right correlations
        AliasMap mapYToW = AliasMap.ofAliases(CorrelationIdentifier.of("y"), CorrelationIdentifier.of("w"));
        assertFalse(ref.containsAllInMemo(refWith3, mapYToW));
    }

    @Test
    void checkCorrelatedWithCommonChild() {
        SyntheticPlannerExpression leaf = new SyntheticPlannerExpression("leaf");
        Reference leafRef = Reference.initialOf(leaf);

        // Create two quantifiers over the leaf
        Quantifier q1 = Quantifier.forEach(leafRef);
        Quantifier q2 = Quantifier.forEach(leafRef);

        // Create two expressions. One of them has q1 as a child and is correlated to q2. The other has
        // q2 as a child and is correlated to q1
        Reference p1Ref = Reference.initialOf(new SyntheticPlannerExpression("parent", ImmutableList.of(q1), ImmutableSet.of(q2.getAlias())));
        Reference p2Ref = Reference.initialOf(new SyntheticPlannerExpression("parent", ImmutableList.of(q2), ImmutableSet.of(q1.getAlias())));
        assertFalse(p1Ref.containsAllInMemo(p2Ref, AliasMap.emptyMap()));
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
            reference.insertFinalExpression(expression);
            assertTrue(reference.containsInMemo(expression, true));
            if (i % 5 == 0) {
                sample.insertFinalExpression(expression);
                assertTrue(sample.containsInMemo(expression, true));
            }
        }

        for (SyntheticPlannerExpression expression : trackingSet) {
            assertTrue(reference.containsInMemo(expression, true));
        }
        assertTrue(reference.containsAllInMemo(sample, AliasMap.emptyMap()));
    }

    @Test
    void memoizeExploratoryLeaf() {
        Reference root = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar"));
        Memoizer memoizer = createMemoizer(root);

        Reference reusedFoo = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("foo"));
        assertSame(root, reusedFoo, "duplicated expression should re-use reference");

        Reference reusedBar = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("bar"));
        assertSame(root, reusedBar, "duplicated expression should re-use reference");

        Reference notReused = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("baz"));
        assertNotSame(root, notReused, "unrelated expression should not re-use reference");

        assertThat(root.getExploratoryExpressions(), contains(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar")));
        assertThat(notReused.getExploratoryExpressions(), contains(new SyntheticPlannerExpression("baz")));
    }

    @Test
    void memoizeExploratoryLeafWithVariations() {
        Reference root = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar"), new SyntheticPlannerExpression("baz"));
        Memoizer memoizer = createMemoizer(root);

        Reference reusedFooBar = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar")));
        assertSame(root, reusedFooBar, "reference should be re-used when it contains all in the memo");

        Reference reusedBarBaz = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("bar"), new SyntheticPlannerExpression("baz")));
        assertSame(root, reusedBarBaz, "reference should be re-used when it contains all in the memo");

        Reference notReused1 = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("qwerty"), new SyntheticPlannerExpression("azerty")));
        assertNotSame(root, notReused1, "reference should not be re-used when none of the elements are in an existing reference");

        Reference notReused2 = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("qwop"), new SyntheticPlannerExpression("asdf")));
        assertNotSame(notReused1, notReused2, "reference should not be re-used when none of the elements are in an existing reference");
        assertNotSame(root, notReused2, "reference should not be re-used when at least one expression in the memo differs");

        // In this case, there are two references (root and notMemoized2) that contain foo, but only the latter contains asdf. Make sure
        // we choose the latter
        Reference reusedFooAsdf = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("asdf")));
        assertSame(notReused2, reusedFooAsdf, "reference containing all variations should be chosen");
    }

    @Test
    void memoizeLeafInTree() {
        Reference ref1 = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar"));
        Reference ref2 = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("asdf"), new SyntheticPlannerExpression("qwerty"), new SyntheticPlannerExpression("azerty"));
        Reference root = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("root", ref1, ref2));
        Memoizer memoizer = createMemoizer(root);

        Reference reusedFoo = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("foo"));
        assertThat(reusedFoo, either(sameInstance(ref1)).or(sameInstance(ref2)));

        Reference reusedBar = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("bar"));
        assertSame(ref1, reusedBar);

        Reference reusedQwerty = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("qwerty"));
        assertSame(ref2, reusedQwerty);

        Reference reusedFooBar = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar")));
        assertSame(ref1, reusedFooBar);

        Reference reusedFooQwerty = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("qwerty")));
        assertSame(ref2, reusedFooQwerty);

        Reference notReusedZop = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("zop"));
        assertThat(notReusedZop, allOf(not(sameInstance(ref1)), not(sameInstance(ref2)), not(sameInstance(root))));

        Reference notReusedBlahYaddaEtc = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("blah"), new SyntheticPlannerExpression("yadda"), new SyntheticPlannerExpression("etc")));
        assertThat(notReusedBlahYaddaEtc, allOf(not(sameInstance(ref1)), not(sameInstance(ref2)), not(sameInstance(root))));

        Reference notReusedBarQwerty = memoizer.memoizeExploratoryExpressions(ImmutableList.of(new SyntheticPlannerExpression("bar"), new SyntheticPlannerExpression("qwerty")));
        assertThat(notReusedBarQwerty, allOf(not(sameInstance(ref1)), not(sameInstance(ref2)), not(sameInstance(root))));
    }

    @Test
    void memoizeWithChildren() {
        Reference ref1 = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar"), new SyntheticPlannerExpression("baz"));
        Reference ref2 = ofExploratoryExpressions(new SyntheticPlannerExpression("qwop"), new SyntheticPlannerExpression("zorp"));
        Reference ref3 = ofExploratoryExpressions(new SyntheticPlannerExpression("whee"), new SyntheticPlannerExpression("wow"), new SyntheticPlannerExpression("woah"));
        Reference inter1 = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("p1", ref1, ref2), SyntheticPlannerExpression.ofRefs("p1", ref1, ref3));
        Reference inter2 = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("p2", ref2, ref3));
        Reference root = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("root", inter1, inter2));
        Memoizer memoizer = createMemoizer(root);

        Reference reusedP1a = memoizer.memoizeExploratoryExpression(SyntheticPlannerExpression.ofRefs("p1", ref1, ref2));
        assertSame(inter1, reusedP1a);

        Reference reusedP1b = memoizer.memoizeExploratoryExpression(SyntheticPlannerExpression.ofRefs("p1", ref1, ref3));
        assertSame(inter1, reusedP1b);

        Reference reusedP2 = memoizer.memoizeExploratoryExpression(SyntheticPlannerExpression.ofRefs("p2", ref2, ref3));
        assertSame(inter2, reusedP2);

        Reference notReused1 = memoizer.memoizeExploratoryExpression(SyntheticPlannerExpression.ofRefs("p3", ref1, ref2));
        assertThat(notReused1, allOf(not(sameInstance(inter1)), not(sameInstance(inter2)), not(sameInstance(root))));

        Reference notReused2 = memoizer.memoizeExploratoryExpression(SyntheticPlannerExpression.ofRefs("p1", ref1));
        assertThat(notReused2, allOf(not(sameInstance(inter1)), not(sameInstance(inter2)), not(sameInstance(root))));
    }

    @Test
    void memoizeGroupWithChildren() {
        Reference ref1 = ofExploratoryExpressions(new SyntheticPlannerExpression("foo"), new SyntheticPlannerExpression("bar"), new SyntheticPlannerExpression("baz"));
        Reference ref2 = ofExploratoryExpressions(new SyntheticPlannerExpression("qwop"), new SyntheticPlannerExpression("zorp"));
        Reference ref3 = ofExploratoryExpressions(new SyntheticPlannerExpression("whee"), new SyntheticPlannerExpression("wow"), new SyntheticPlannerExpression("woah"));
        Reference ref4 = ofExploratoryExpressions(new SyntheticPlannerExpression("yea"), new SyntheticPlannerExpression("yep"), new SyntheticPlannerExpression("yes"));
        Reference inter1 = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("p1", ref1, ref2), SyntheticPlannerExpression.ofRefs("p1", ref1, ref3), SyntheticPlannerExpression.ofRefs("p1", ref2, ref4));
        Reference inter2 = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("p2", ref2, ref3));
        Reference root = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("root", inter1, inter2));
        final Memoizer memoizer = createMemoizer(root);

        Reference reusedP1a = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                SyntheticPlannerExpression.ofRefs("p1", ref1, ref2),
                SyntheticPlannerExpression.ofRefs("p1", ref2, ref4)
        ));
        assertSame(inter1, reusedP1a);

        Reference reusedP1b = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                SyntheticPlannerExpression.ofRefs("p1", ref1, ref3),
                SyntheticPlannerExpression.ofRefs("p1", ref2, ref4)
        ));
        assertSame(inter1, reusedP1b);

        Reference notReusedP1 = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                SyntheticPlannerExpression.ofRefs("p1", ref3, ref4), // not in inter1
                SyntheticPlannerExpression.ofRefs("p1", ref1, ref2),
                SyntheticPlannerExpression.ofRefs("p1", ref1, ref3)

        ));
        assertThat(notReusedP1, allOf(not(sameInstance(inter1)), not(sameInstance(inter2))));

        Reference notReusedMixed = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                SyntheticPlannerExpression.ofRefs("p1", ref1, ref3),
                SyntheticPlannerExpression.ofRefs("p2", ref2, ref3)
        ));
        assertThat(notReusedMixed, allOf(not(sameInstance(inter1)), not(sameInstance(inter2))));
    }

    @Test
    void doNotReuseWhenCorrelationsAreFlipped() {
        Reference leafRef = ofExploratoryExpressions(new SyntheticPlannerExpression("leaf"));
        Quantifier leafQ1 = Quantifier.forEach(leafRef);
        Quantifier leafQ2 = Quantifier.forEach(leafRef);
        Reference parent1 = ofExploratoryExpressions(new SyntheticPlannerExpression("p1", ImmutableList.of(leafQ1), ImmutableSet.of(leafQ2.getAlias())));
        Reference parent2 = ofExploratoryExpressions(new SyntheticPlannerExpression("p2", ImmutableList.of(leafQ2), ImmutableSet.of(leafQ1.getAlias())));
        Reference root = ofExploratoryExpressions(SyntheticPlannerExpression.ofRefs("root", parent1, parent2));
        final Memoizer memoizer = createMemoizer(root);

        // Do re-use the reference if the correlations align
        Reference reusedP1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("p1", ImmutableList.of(leafQ1), ImmutableSet.of(leafQ2.getAlias())));
        assertSame(parent1, reusedP1);
        Reference reusedP2 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("p2", ImmutableList.of(leafQ2), ImmutableSet.of(leafQ1.getAlias())));
        assertSame(parent2, reusedP2);

        // Do not re-use the reference if the correlations are backwards
        Reference notReusedP1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("p1", ImmutableList.of(leafQ2), ImmutableSet.of(leafQ1.getAlias())));
        assertThat(notReusedP1, allOf(not(sameInstance(parent1)), not(sameInstance(parent2))));
        Reference notReusedP2 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("p2", ImmutableList.of(leafQ1), ImmutableSet.of(leafQ2.getAlias())));
        assertThat(notReusedP2, allOf(not(sameInstance(parent1)), not(sameInstance(parent2))));
    }

    @Test
    void doNotReuseLeafReferenceWithExtraCorrelations() {
        final SyntheticPlannerExpression correlatedExpr = SyntheticPlannerExpression.withCorrelated("p1", "a");
        final SyntheticPlannerExpression uncorrelatedExpr = SyntheticPlannerExpression.withCorrelated("p2");
        Reference root = ofExploratoryExpressions(correlatedExpr, uncorrelatedExpr);
        final Memoizer memoizer = createMemoizer(root);

        Reference reusedRoot = memoizer.memoizeExploratoryExpression(correlatedExpr);
        assertSame(root, reusedRoot, "correlated expression should re-use memoized expression");

        // When constructing a memoized reference, do not allow us to re-use the expression if there are correlations
        // missing in the new set that are present in the original reference, as it won't be allowed in all of the same
        // places
        Reference notReusedRoot = memoizer.memoizeExploratoryExpression(uncorrelatedExpr);
        assertNotSame(root, notReusedRoot, "uncorrelated expression should not re-use memoized expression");
        assertEquals(1, notReusedRoot.getTotalMembersSize(), "non-memoized reference should only contain one member");
        assertEquals(ImmutableSet.of(), notReusedRoot.getCorrelatedTo());
    }

    @Test
    void doNotReuseReferenceWithExtraCorrelations() {
        final Quantifier baseQun = Quantifier.forEach(ofExploratoryExpressions(new SyntheticPlannerExpression("base")));
        final Quantifier otherQun = Quantifier.forEach(ofExploratoryExpressions(new SyntheticPlannerExpression("other")));
        final Reference middle = ofExploratoryExpressions(
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(otherQun.getAlias())),
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of())
        );
        final Quantifier middleQun = Quantifier.forEach(middle);
        final Reference root = ofExploratoryExpressions(new SyntheticPlannerExpression("root", ImmutableList.of(otherQun, middleQun), ImmutableList.of()));
        final Memoizer memoizer = createMemoizer(root);

        // Do not re-use a reference if for the uncorrelated expression
        Reference newRef1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of()));
        assertNotSame(middle, newRef1, "reference missing correlation should not re-use correlated reference");
        assertEquals(1, newRef1.getTotalMembersSize());

        // Do re-use the reference if there is a correlation that matches the existing expression. Note that the other expression comes along
        Reference reused1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(otherQun.getAlias())));
        assertSame(middle, reused1, "when correlation sets match, the reference should be re-used");

        // Do re-use the reference if we get a mix of both, but the total correlation set matches the expected set
        Reference reused2 = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of()),
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(otherQun.getAlias()))));
        assertSame(middle, reused2);
    }

    @Test
    void matchCorrelationsSplitAcrossExpressions() {
        final Quantifier baseQun = Quantifier.forEach(ofExploratoryExpressions(new SyntheticPlannerExpression("base")));
        final Quantifier other1 = Quantifier.forEach(ofExploratoryExpressions(new SyntheticPlannerExpression("o1")));
        final Quantifier other2 = Quantifier.forEach(ofExploratoryExpressions(new SyntheticPlannerExpression("o2")));
        final Reference middle = ofExploratoryExpressions(
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other1.getAlias())),
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other2.getAlias())),
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other1.getAlias(), other2.getAlias()))
        );
        final Quantifier middleQun = Quantifier.forEach(middle);
        final Reference root =  ofExploratoryExpressions(new SyntheticPlannerExpression("root", ImmutableList.of(other1, other2, middleQun), ImmutableSet.of()));
        final Memoizer memoizer = createMemoizer(root);

        Reference newRef1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other1.getAlias())));
        assertNotSame(middle, newRef1);

        Reference newRef2 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other2.getAlias())));
        assertNotSame(middle, newRef2);

        Reference reused1 = memoizer.memoizeExploratoryExpression(new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other1.getAlias(), other2.getAlias())));
        assertSame(middle, reused1);

        Reference reused2 = memoizer.memoizeExploratoryExpressions(ImmutableList.of(
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other1.getAlias())),
                new SyntheticPlannerExpression("middle", ImmutableList.of(baseQun), ImmutableSet.of(other2.getAlias()))
        ));
        assertSame(middle, reused2);
    }

    /**
     * A mock planner expression with very general semantics to test the correctness of various operations on the memo
     * data structure.
     */
    private static class SyntheticPlannerExpression extends AbstractRelationalExpressionWithChildren {
        @Nonnull
        private final String identity;
        @Nonnull
        private final List<? extends Quantifier> quantifiers;
        private final Set<CorrelationIdentifier> correlatedTo;

        public SyntheticPlannerExpression(@Nonnull String identity) {
            this(identity, ImmutableList.of());
        }

        public SyntheticPlannerExpression(@Nonnull String identity, @Nonnull Collection<? extends Quantifier> children) {
            this(identity, children, ImmutableSet.of());
        }

        public SyntheticPlannerExpression(@Nonnull String identity,
                                          @Nonnull Collection<? extends Quantifier> children,
                                          @Nonnull Collection<CorrelationIdentifier> correlatedTo) {
            this.identity = identity;
            this.quantifiers = ImmutableList.copyOf(children);
            this.correlatedTo = ImmutableSet.copyOf(correlatedTo);
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
        public int computeHashCodeWithoutChildren() {
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
                children.add(Reference.initialOf(generate(random, maxDepth - 1)));
            }
            return new SyntheticPlannerExpression(name, Quantifiers.forEachQuantifiers(children));
        }

        @Override
        public int getRelationalChildCount() {
            return quantifiers.size();
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
            return correlatedTo;
        }

        @Nonnull
        @Override
        public SyntheticPlannerExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                final boolean shouldSimplifyValues,
                                                                @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
            return new SyntheticPlannerExpression(identity, translatedQuantifiers);
        }

        @Nonnull
        @Override
        public Value getResultValue() {
            return new QueriedValue();
        }

        @Override
        public String toString() {
            return "SyntheticPlannerExpression(" + identity
                    + (quantifiers.isEmpty() ? "" : (", children: " + quantifiers.stream().map(qun -> qun.getAlias().toString()).collect(Collectors.joining(", ", "[", "]"))))
                    + ")";
        }

        @Nonnull
        public static SyntheticPlannerExpression ofRefs(@Nonnull String name, @Nonnull Reference... references) {
            return new SyntheticPlannerExpression(name, Quantifiers.forEachQuantifiers(List.of(references)));
        }

        @Nonnull
        public static SyntheticPlannerExpression withCorrelated(@Nonnull String name, @Nonnull String... correlatedTo) {
            return new SyntheticPlannerExpression(name, ImmutableList.of(), Arrays.stream(correlatedTo).map(CorrelationIdentifier::of).collect(ImmutableSet.toImmutableSet()));
        }
    }
}
