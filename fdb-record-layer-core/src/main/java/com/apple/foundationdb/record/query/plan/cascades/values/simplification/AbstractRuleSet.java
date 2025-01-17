/*
 * AbstractRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 * @param <RESULT> the type that {@link AbstractValueRule}s in this set yield
 * @param <CALL> the type of the call rules in this set will receive
 *        when {@link com.apple.foundationdb.record.query.plan.cascades.PlannerRule#onMatch(PlannerRuleCall)} is invoked.
 * @param <BASE> the type of entity all rules in the set must match
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class AbstractRuleSet<RESULT, CALL extends AbstractRuleCall<RESULT, CALL, BASE>, BASE> {
    @Nonnull
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // false positive
    private final Multimap<Class<?>, AbstractRule<RESULT, CALL, BASE, ? extends BASE>> ruleIndex;
    @Nonnull
    private final List<AbstractRule<RESULT, CALL, BASE, ? extends BASE>> alwaysRules;

    @Nonnull
    private final SetMultimap<AbstractRule<RESULT, CALL, BASE, ? extends BASE>, AbstractRule<RESULT, CALL, BASE, ? extends BASE>> dependsOn;

    @Nonnull
    private final LoadingCache<Class<? extends BASE>, List<AbstractRule<RESULT, CALL, BASE, ? extends BASE>>> rulesCache;

    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    protected AbstractRuleSet(@Nonnull final Set<? extends AbstractRule<RESULT, CALL, BASE, ? extends BASE>> rules,
                              @Nonnull final SetMultimap<? extends AbstractRule<RESULT, CALL, BASE, ? extends BASE>, ? extends AbstractRule<RESULT, CALL, BASE, ? extends BASE>> dependencies) {
        this.ruleIndex = MultimapBuilder.hashKeys().arrayListValues().build();
        this.alwaysRules = new ArrayList<>();
        this.dependsOn = MultimapBuilder.hashKeys().hashSetValues().build();
        for (final var rule : rules) {
            Optional<Class<?>> root = rule.getRootOperator();
            if (root.isPresent()) {
                ruleIndex.put(root.get(), rule);
            } else {
                alwaysRules.add(rule);
            }
        }

        this.dependsOn.putAll(dependencies);

        this.rulesCache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .build(new CacheLoader<>() {
                    @Nonnull
                    @Override
                    @SuppressWarnings("UnstableApiUsage")
                    public List<AbstractRule<RESULT, CALL, BASE, ? extends BASE>> load(@Nonnull final Class<? extends BASE> key) {
                        final var applicableRules =
                                ImmutableSet.<AbstractRule<RESULT, CALL, BASE, ? extends BASE>>builderWithExpectedSize(ruleIndex.size() + alwaysRules.size())
                                        .addAll(ruleIndex.get(key))
                                        .addAll(alwaysRules)
                                        .build();
                        if (applicableRules.isEmpty()) {
                            return ImmutableList.of();
                        }
                        return TopologicalSort.anyTopologicalOrderPermutation(PartiallyOrderedSet.of(applicableRules, dependsOn)).orElseThrow(() -> new RecordCoreException("circular dependency among simplification rules"));
                    }
                });
    }

    @Nonnull
    public Stream<? extends AbstractRule<RESULT, CALL, BASE, ? extends BASE>> getValueRules(@Nonnull BASE value) {
        return getValueRules(value, r -> true);
    }

    @Nonnull
    @SuppressWarnings({"PMD.PreserveStackTrace", "unchecked"})
    public Stream<? extends AbstractRule<RESULT, CALL, BASE, ? extends BASE>> getValueRules(@Nonnull BASE value,
                                                                                            @Nonnull final Predicate<AbstractRule<RESULT, CALL, BASE, ? extends BASE>> rulePredicate) {
        try {
            return rulesCache.get((Class<? extends BASE>)value.getClass()).stream().filter(rulePredicate);
        } catch (final ExecutionException ee) {
            throw new RecordCoreException(ee.getCause());
        }
    }
}
