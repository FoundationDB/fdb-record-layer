/*
 * AliasMapTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testcase class for {@link AliasMap}.
 */
public class AliasMapTest {
    @Test
    public void testBuilder() {
        final AliasMap aliasMap =
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                .build();

        assertTrue(aliasMap.containsSource(of("a")));
        assertTrue(aliasMap.containsTarget(of("c")));
        assertTrue(aliasMap.containsSource(of("b")));
        assertTrue(aliasMap.containsTarget(of("d")));

        assertEquals(of("c"), aliasMap.getTarget(of("a")));
        assertEquals(of("d"), aliasMap.getTarget(of("b")));
    }

    @Test
    public void testDerived() {
        final AliasMap aliasMap =
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                        .build();

        final AliasMap newAliasMap = aliasMap.derived()
                .put(of("x"), of("y"))
                .build();

        assertTrue(newAliasMap.containsSource(of("a")));
        assertTrue(newAliasMap.containsTarget(of("c")));
        assertTrue(newAliasMap.containsSource(of("b")));
        assertTrue(newAliasMap.containsTarget(of("d")));
        assertTrue(newAliasMap.containsSource(of("x")));
        assertTrue(newAliasMap.containsTarget(of("y")));

        Assertions.assertFalse(aliasMap.containsSource(of("x")));

        assertEquals(of("c"), newAliasMap.getTarget(of("a")));
        assertEquals(of("d"), newAliasMap.getTarget(of("b")));
        assertEquals(of("y"), newAliasMap.getTarget(of("x")));
    }

    @Test
    public void testZip() {
        final AliasMap aliasMap = AliasMap.zip(ImmutableList.of(of("a"), of("b")), ImmutableList.of(of("c"), of("d")));

        assertTrue(aliasMap.containsSource(of("a")));
        assertTrue(aliasMap.containsTarget(of("c")));
        assertTrue(aliasMap.containsSource(of("b")));
        assertTrue(aliasMap.containsTarget(of("d")));

        assertEquals(of("c"), aliasMap.getTarget(of("a")));
        assertEquals(of("d"), aliasMap.getTarget(of("b")));
    }

    @Test
    public void testCombine() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("u"), of("w"))
                        .put(of("v"), of("x"))
                        .build();

        final AliasMap combinedMap = leftMap.combine(rightMap);

        assertTrue(combinedMap.containsSource(of("a")));
        assertTrue(combinedMap.containsTarget(of("c")));
        assertTrue(combinedMap.containsSource(of("b")));
        assertTrue(combinedMap.containsTarget(of("d")));
        assertTrue(combinedMap.containsSource(of("u")));
        assertTrue(combinedMap.containsTarget(of("w")));
        assertTrue(combinedMap.containsSource(of("v")));
        assertTrue(combinedMap.containsTarget(of("x")));

        assertEquals(of("c"), combinedMap.getTarget(of("a")));
        assertEquals(of("d"), combinedMap.getTarget(of("b")));
        assertEquals(of("w"), combinedMap.getTarget(of("u")));
        assertEquals(of("x"), combinedMap.getTarget(of("v")));
    }

    @Test
    public void testCombineIncompatible1() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("a"), of("w"))
                        .put(of("v"), of("x"))
                        .build();

        Assertions.assertFalse(leftMap.combineMaybe(rightMap).isPresent());
        Assertions.assertThrows(IllegalArgumentException.class, () -> leftMap.combine(rightMap));
    }

    @Test
    public void testCombineIncompatible2() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("u"), of("c"))
                        .put(of("v"), of("x"))
                        .build();

        Assertions.assertFalse(leftMap.combineMaybe(rightMap).isPresent());
        Assertions.assertThrows(IllegalArgumentException.class, () -> leftMap.combine(rightMap));
    }

    @Test
    void testCompose() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .put(of("b"), of("j"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("i"), of("u"))
                        .put(of("j"), of("v"))
                        .build();

        final AliasMap combinedMap = leftMap.compose(rightMap);

        assertEquals(AliasMap.builder()
                        .put(of("a"), of("u"))
                        .put(of("b"), of("v"))
                        .build(),
                combinedMap);
    }

    @Test
    void testComposeDanglingTargets() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .put(of("b"), of("j"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("i"), of("u"))
                        .build();

        final AliasMap combinedMap = leftMap.compose(rightMap);

        assertEquals(AliasMap.builder()
                        .put(of("a"), of("u"))
                        .put(of("b"), of("j"))
                        .build(),
                combinedMap);
    }

    @Test
    void testComposeDanglingSources() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("i"), of("u"))
                        .put(of("j"), of("v"))
                        .build();

        final AliasMap combinedMap = leftMap.compose(rightMap);

        assertEquals(AliasMap.builder()
                        .put(of("a"), of("u"))
                        .put(of("j"), of("v"))
                        .build(),
                combinedMap);
    }

    @Test
    void testComposeDisjointMaps() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("j"), of("v"))
                        .build();

        final AliasMap combinedMap = leftMap.compose(rightMap);

        assertEquals(AliasMap.builder()
                        .put(of("a"), of("i"))
                        .put(of("j"), of("v"))
                        .build(),
                combinedMap);
    }

    @Test
    void testComposeIncompatibleMaps() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("a"), of("v"))
                        .build();

        assertThrows(IllegalArgumentException.class, () -> leftMap.compose(rightMap));
    }

    @Test
    void testComposeIncompatibleMaps2() {
        final AliasMap leftMap =
                AliasMap.builder()
                        .put(of("a"), of("i"))
                        .build();

        final AliasMap rightMap =
                AliasMap.builder()
                        .put(of("b"), of("i"))
                        .build();

        assertThrows(IllegalArgumentException.class, () -> leftMap.compose(rightMap));
    }

    @Test
    void testMatchNoCorrelations() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));
        
        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> ImmutableSet.of(),
                        right,
                        r -> ImmutableSet.of(),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("z"))
                        .put(of("c"), of("y"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("y"))
                        .put(of("b"), of("x"))
                        .put(of("c"), of("z"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("y"))
                        .put(of("b"), of("z"))
                        .put(of("c"), of("x"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("z"))
                        .put(of("b"), of("x"))
                        .put(of("c"), of("y"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("z"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("x"))
                        .build()),
                matches);
    }

    @Test
    void testMatchNoCorrelations1() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("c"), of("d"));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> ImmutableSet.of(),
                        right,
                        r -> ImmutableSet.of(),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("c"))
                        .put(of("b"), of("d"))
                        .build(),
                AliasMap.builder()
                        .put(of("b"), of("c"))
                        .put(of("a"), of("d"))
                        .build()),
                matches);
    }

    @Test
    void testMatchSomeCorrelations1() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build()),
                matches);
    }

    @Test
    void testMatchSomeCorrelations2() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"), of("u"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("c"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("z"), ImmutableSet.of(of("y")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("u"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .put(of("d"), of("x"))
                        .build(),
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .put(of("d"), of("u"))
                        .build()),
                matches);
    }

    @Test
    void testMatchFullCorrelations() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a")),
                        of("c"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")),
                        of("z"), ImmutableSet.of(of("y")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build()),
                matches);
    }

    @Test
    void testMatchIncompatibleCorrelations() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a")),
                        of("c"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")),
                        of("z"), ImmutableSet.of(of("x")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(), matches);
    }

    @Test
    void testMatchEmpty() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of();
        final Set<CorrelationIdentifier> right = ImmutableSet.of();

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of();

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of();

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(AliasMap.emptyMap()), matches);
    }

    @Test
    void testMatchExternalCorrelations1() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a"), of("i")),
                        of("c"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")),
                        of("z"), ImmutableSet.of(of("y")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build()),
                matches);
    }

    @Test
    void testMatchExternalCorrelations2() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a")),
                        of("c"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")),
                        of("z"), ImmutableSet.of(of("y"), of("i")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build()),
                matches);
    }

    @Test
    void testMatchExternalCorrelations3() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("a"), of("b"), of("c"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("x"), of("y"), of("z"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("b"), ImmutableSet.of(of("a")),
                        of("c"), ImmutableSet.of(of("b")),
                        of("i"), ImmutableSet.of(of("b")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("y"), ImmutableSet.of(of("x")),
                        of("z"), ImmutableSet.of(of("y")),
                        of("j"), ImmutableSet.of(of("u")));

        final Iterable<AliasMap> matchIterable = AliasMap.emptyMap()
                .findCompleteMatches(
                        left,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> dependenciesRight.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> true);

        final ImmutableSet<AliasMap> matches = ImmutableSet.copyOf(matchIterable);

        assertEquals(ImmutableSet.of(
                AliasMap.builder()
                        .put(of("a"), of("x"))
                        .put(of("b"), of("y"))
                        .put(of("c"), of("z"))
                        .build()),
                matches);
    }
}
