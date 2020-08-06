/*
 * ExpressionRefDelegate.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An expression reference that wraps another expression reference.
 * @param <T> the type
 */
@API(API.Status.EXPERIMENTAL)
public class ExpressionRefDelegate<T extends RelationalExpression> implements ExpressionRef<T> {
    private final ExpressionRef<T> delegate;

    protected ExpressionRefDelegate(final ExpressionRef<T> delegate) {
        this.delegate = delegate;
    }

    @Nonnull
    public ExpressionRef<T> getDelegate() {
        return delegate;
    }

    @Override
    public boolean insert(@Nonnull final T newValue) {
        return delegate.insert(newValue);
    }

    @Nonnull
    @Override
    public T get() {
        return delegate.get();
    }

    @Nullable
    @Override
    public <U> U acceptPropertyVisitor(@Nonnull final PlannerProperty<U> property) {
        return delegate.acceptPropertyVisitor(property);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindWithin(@Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return delegate.bindWithin(matcher);
    }

    @Nonnull
    @Override
    public <U extends RelationalExpression> ExpressionRef<U> map(@Nonnull final Function<T, U> func) {
        return delegate.map(func);
    }

    @Nullable
    @Override
    public <U extends RelationalExpression> ExpressionRef<U> flatMapNullable(@Nonnull final Function<T, ExpressionRef<U>> nullableFunc) {
        return delegate.flatMapNullable(nullableFunc);
    }

    @Nonnull
    @Override
    public ExpressionRef<T> getNewRefWith(@Nonnull final T expression) {
        return delegate.getNewRefWith(expression);
    }

    @Override
    public boolean containsAllInMemo(@Nonnull final ExpressionRef<? extends RelationalExpression> otherRef, @Nonnull final AliasMap equivalenceMap) {
        return delegate.containsAllInMemo(otherRef, equivalenceMap);
    }

    @Override
    public RelationalExpressionPointerSet<T> getMembers() {
        return delegate.getMembers();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return delegate.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public ExpressionRef<T> rebase(@Nonnull final AliasMap translationMap) {
        return delegate.rebase(translationMap);
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        return delegate.semanticEquals(other, equivalenceMap);
    }

    @Override
    public int semanticHashCode() {
        return delegate.semanticHashCode();
    }
}
