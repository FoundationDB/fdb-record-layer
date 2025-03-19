/*
 * LogicalOperators.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@API(API.Status.EXPERIMENTAL)
public class LogicalOperators implements Iterable<LogicalOperator> {

    @Nonnull
    private static final LogicalOperators EMPTY = new LogicalOperators(ImmutableList.of());

    @Nonnull
    private final List<LogicalOperator> underlying;

    public LogicalOperators(@Nonnull Iterable<LogicalOperator> underlying) {
        this.underlying = ImmutableList.copyOf(underlying);
    }

    @Nonnull
    @Override
    public Iterator<LogicalOperator> iterator() {
        return underlying.iterator();
    }

    @Nonnull
    public Stream<LogicalOperator> stream() {
        return underlying.stream();
    }

    @Nonnull
    public List<LogicalOperator> asList() {
        return underlying;
    }

    @Nonnull
    public LogicalOperator first() {
        Assert.thatUnchecked(!underlying.isEmpty());
        return underlying.get(0);
    }

    public boolean isEmpty() {
        return underlying.isEmpty();
    }

    public int size() {
        return  underlying.size();
    }

    @Nonnull
    public Set<CorrelationIdentifier> getCorrelations() {
        return underlying.stream().map(operator -> operator.getQuantifier().getAlias()).collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    public List<Quantifier> getQuantifiers() {
        return underlying.stream().map(LogicalOperator::getQuantifier).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public Expressions getExpressions() {
        Expressions accumulated = Expressions.empty();
        for (final var logicalOperator : this) {
            accumulated = accumulated.concat(Expressions.of(logicalOperator.getOutput()));
        }
        return accumulated;
    }

    @Nonnull
    public LogicalOperators forEachOnly() {
        return LogicalOperators.of(this.stream()
                .filter(operator -> operator.getQuantifier().getClass().equals(Quantifier.ForEach.class))
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public LogicalOperators concat(@Nonnull LogicalOperator logicalOperator) {
        return concat(LogicalOperators.ofSingle(logicalOperator));
    }

    @Nonnull
    public LogicalOperators concat(@Nonnull LogicalOperators other) {
        return new LogicalOperators(Iterables.concat(this, other));
    }

    @Nonnull
    public static LogicalOperators of(@Nonnull Iterable<LogicalOperator> logicalOperators) {
        return new LogicalOperators(logicalOperators);
    }

    @Nonnull
    public static LogicalOperators ofSingle(@Nonnull LogicalOperator logicalOperator) {
        return new LogicalOperators(ImmutableList.of(logicalOperator));
    }

    @Nonnull
    public static LogicalOperators empty() {
        return EMPTY;
    }
}
