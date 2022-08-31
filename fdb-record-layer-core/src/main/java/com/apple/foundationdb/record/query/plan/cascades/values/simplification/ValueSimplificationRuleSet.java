/*
 * ValueSimplificationRuleSet.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
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
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class ValueSimplificationRuleSet {
    @Nonnull
    protected static final ValueSimplificationRule<? extends Value> composeFieldValueOverRecordConstructorRule = new ComposeFieldValueOverRecordConstructorRule();
    protected static final Set<ValueSimplificationRule<? extends Value>> SIMPLIFICATION_RULES =
            ImmutableSet.of(composeFieldValueOverRecordConstructorRule);

    protected static final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> SIMPLIFICATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    @Nonnull
    private final Multimap<Class<?>, ValueSimplificationRule<? extends Value>> ruleIndex =
            MultimapBuilder.hashKeys().arrayListValues().build();
    @Nonnull
    private final List<ValueSimplificationRule<? extends Value>> alwaysRules = new ArrayList<>();

    @Nonnull
    private final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> dependsOn =
            MultimapBuilder.SetMultimapBuilder.hashKeys().hashSetValues().build();

    @Nonnull
    private final LoadingCache<Class<? extends Value>, List<ValueSimplificationRule<? extends Value>>> rulesCache =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .build(new CacheLoader<>() {
                        @SuppressWarnings("UnstableApiUsage")
                        public List<ValueSimplificationRule<? extends Value>> load(@Nonnull final Class<? extends Value> key) {
                            final var applicableRules =
                                    ImmutableSet.<ValueSimplificationRule<? extends Value>>builderWithExpectedSize(ruleIndex.size() + alwaysRules.size())
                                            .addAll(ruleIndex.get(key))
                                            .addAll(alwaysRules)
                                            .build();
                            if (applicableRules.isEmpty()) {
                                return ImmutableList.of();
                            }
                            return TopologicalSort.anyTopologicalOrderPermutation(PartialOrder.of(applicableRules, dependsOn)).orElseThrow(() -> new RecordCoreException("circular dependency among simplification rules"));
                        }
                    });

    protected ValueSimplificationRuleSet(@Nonnull final Set<ValueSimplificationRule<? extends Value>> rules,
                                         @Nonnull final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> dependsOn) {
        for (final var rule : rules) {
            Optional<Class<?>> root = rule.getRootOperator();
            if (root.isPresent()) {
                ruleIndex.put(root.get(), rule);
            } else {
                alwaysRules.add(rule);
            }
        }

        this.dependsOn.putAll(dependsOn);
    }

    @Nonnull
    public Stream<ValueSimplificationRule<? extends Value>> getValueRules(@Nonnull Value value) {
        return getValueRules(value, r -> true);
    }

    @Nonnull
    public Stream<ValueSimplificationRule<? extends Value>> getValueRules(@Nonnull Value value,
                                                                          @Nonnull final Predicate<ValueSimplificationRule<? extends Value>> rulePredicate) {
        try {
            return rulesCache.get(value.getClass()).stream().filter(rulePredicate);
        } catch (final ExecutionException ee) {
            throw new RecordCoreException(ee.getCause());
        }
    }

    public static ValueSimplificationRuleSet ofSimplificationRules() {
        return new ValueSimplificationRuleSet(SIMPLIFICATION_RULES, SIMPLIFICATION_DEPENDS_ON);
    }
}
