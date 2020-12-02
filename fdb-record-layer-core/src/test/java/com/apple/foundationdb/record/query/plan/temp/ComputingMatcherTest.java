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

import com.apple.foundationdb.record.query.plan.temp.matching.BoundMatch;
import com.apple.foundationdb.record.query.plan.temp.matching.ComputingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matching.GenericMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier.of;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Testcase class for {@link com.apple.foundationdb.record.query.plan.temp.matching.ComputingMatcher}.
 */
public class ComputingMatcherTest {
    @Test
    void testMatchNoCorrelations1() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("l1"), of("l2"), of("l3"), of("ll4"), of("ll5"), of("ll6"), of("ll7"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("r1"), of("r2"), of("r3"), of("rr4"), of("rr5"), of("rr6"), of("rr7"));

        final GenericMatcher<BoundMatch<EnumeratingIterable<String>>> matcher =
                ComputingMatcher.onAliasDependencies(
                        AliasMap.emptyMap(),
                        left,
                        l -> l,
                        l -> ImmutableSet.of(),
                        right,
                        r -> r,
                        r -> ImmutableSet.of(),
                        (l, r, aliasMap) -> {
                            if (l.toString().substring(1).equals(r.toString().substring(1))) {
                                return ImmutableList.of(l + " + " + r);
                            }
                            return ImmutableList.of();
                        },
                        ComputingMatcher::productAccumulator);

        final Iterable<BoundMatch<EnumeratingIterable<String>>> matches = matcher.match();


        matches.forEach(boundMatch -> System.out.println(boundMatch.getAliasMap()));

        System.out.println("================");

        final Set<AliasMap> distinctMatches = StreamSupport.stream(matches.spliterator(), false)
                .map(BoundMatch::getAliasMap)
                .collect(Collectors.toSet());

        distinctMatches.forEach(System.out::println);
    }

    @Test
    void testMatchNoCorrelations2() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("l1"), of("l2"), of("l3"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("r1"), of("r2"), of("r3"), of("rr4"), of("rr5"), of("rr6"), of("rr7"));

        final GenericMatcher<BoundMatch<EnumeratingIterable<String>>> matcher =
                ComputingMatcher.onAliasDependencies(
                        AliasMap.emptyMap(),
                        left,
                        l -> l,
                        l -> ImmutableSet.of(),
                        right,
                        r -> r,
                        r -> ImmutableSet.of(),
                        (l, r, aliasMap) -> {
                            if (l.toString().substring(1).equals(r.toString().substring(1))) {
                                return ImmutableList.of(l + " + " + r);
                            }
                            return ImmutableList.of();
                        },
                        ComputingMatcher::productAccumulator);

        final Iterable<BoundMatch<EnumeratingIterable<String>>> matches = matcher.match();

        matches.forEach(boundMatch -> System.out.println(boundMatch.getAliasMap()));

        System.out.println("================");

        final Set<AliasMap> distinctMatches = StreamSupport.stream(matches.spliterator(), false)
                .map(BoundMatch::getAliasMap)
                .collect(Collectors.toSet());

        distinctMatches.forEach(System.out::println);
    }

    @Test
    void testMatchSomeCorrelations1() {
        final Set<CorrelationIdentifier> left = ImmutableSet.of(of("m1"), of("m2"), of("m3"), of("a"));
        final Set<CorrelationIdentifier> right = ImmutableSet.of(of("m2"), of("x"), of("m1"), of("m3"));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesLeft =
                ImmutableMap.of(of("a"), ImmutableSet.of(of("m1"), of("m2"), of("m3")));

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependenciesRight =
                ImmutableMap.of(of("x"), ImmutableSet.of(of("m1"), of("m2"), of("m3")));

        final GenericMatcher<BoundMatch<EnumeratingIterable<String>>> matcher =
                ComputingMatcher.onAliasDependencies(
                        AliasMap.emptyMap(),
                        left,
                        l -> l,
                        l -> dependenciesLeft.getOrDefault(l, ImmutableSet.of()),
                        right,
                        r -> r,
                        r -> dependenciesLeft.getOrDefault(r, ImmutableSet.of()),
                        (l, r, aliasMap) -> {
                            if (l.toString().substring(1).equals(r.toString().substring(1))) {
                                return ImmutableList.of(l + " + " + r);
                            }
                            return ImmutableList.of();
                        },
                        ComputingMatcher::productAccumulator);

        final Iterable<BoundMatch<EnumeratingIterable<String>>> matches = matcher.match();

        final Set<AliasMap> distinctMatches = StreamSupport.stream(matches.spliterator(), false)
                .map(BoundMatch::getAliasMap)
                .collect(Collectors.toSet());

        matches.forEach(boundMatch -> {
            System.out.println(boundMatch.getAliasMap());
            //    System.out.println("  " + ImmutableSet.copyOf(boundMatch.getMatchResultOptional().get()));
        });

        System.out.println("==================");
        distinctMatches.forEach(System.out::println);
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
                .findMatches(
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
                .findMatches(
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
                .findMatches(
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
                .findMatches(
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
                .findMatches(
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
                .findMatches(
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
                .findMatches(
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
