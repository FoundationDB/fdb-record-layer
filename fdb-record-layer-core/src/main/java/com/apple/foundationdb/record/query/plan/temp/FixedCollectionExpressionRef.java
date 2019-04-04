/*
 * FixedCollectionExpressionRef.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A mutable reference to a collection of planner expressions with fixed size.
 *
 * <p>
 * A {@code FixedCollectionExpressionRef} is a reference that contains more than one member. Unlike a full
 * Cascades-style group reference, the number of members is fixed at the time of creation. Furthermore, each member
 * is a fixed {@link SingleExpressionRef}. As a result, the only modifications permitted within the reference are
 * substitutions of one member for another member within the same {@code SingleExpressionRef}. These limited semantics
 * can be handled by a rewrite planner without major modifications or the complexity of a full Cascades-style planner.
 * </p>
 *
 * <p>
 * Note that the {@link FixedCollectionExpressionRef} does not do any kind of memoization. Care should be taken to
 * minimize the number of members of the group to avoid using too much memory.
 * </p>
 *
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public class FixedCollectionExpressionRef<T extends PlannerExpression> implements ExpressionRef<T> {
    private final List<SingleExpressionRef<T>> expressionRefs;

    public FixedCollectionExpressionRef(@Nonnull Collection<T> expressions) {
        this(expressions.stream().map(SingleExpressionRef::of).collect(Collectors.toList()));
    }

    private FixedCollectionExpressionRef(@Nonnull List<SingleExpressionRef<T>> expressionRefs) {
        this.expressionRefs = expressionRefs;
    }

    @Nonnull
    @Override
    public T get() {
        throw new UngettableReferenceException("tried to call get() on a FixedCollectionExpressionRef");
    }

    @Nonnull
    public List<SingleExpressionRef<T>> getMembers() {
        return expressionRefs;
    }

    @Override
    public <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> property) {
        if (property.shouldVisit(this)) {
            final List<U> memberResults = new ArrayList<>(expressionRefs.size());
            for (SingleExpressionRef<T> expression : expressionRefs) {
                memberResults.add(expression.get().acceptPropertyVisitor(property));
            }
            return property.evaluateAtRef(this, memberResults);
        }
        return null;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindWithin(@Nonnull ExpressionMatcher<? extends Bindable> matcher) {
        Stream.Builder<Stream<PlannerBindings>> memberStreams = Stream.builder();
        for (SingleExpressionRef<T> expressionRef : expressionRefs) {
            memberStreams.add(expressionRef.get().bindTo(matcher));
        }
        return memberStreams.build().flatMap(Function.identity()); // concat
    }
}
