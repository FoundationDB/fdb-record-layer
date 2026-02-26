/*
 * PlannerBindings.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A map-like structure that supports a map from a binding to a collection of objects, such as
 * {@link RelationalExpression}s and {@link Reference}s. A binding's
 * key is a pointer to the {@link BindingMatcher} that created the binding, eliminating the need for a unique string
 * or symbol identifier. A {@code PlannerBindings} is immutable but has a {@link Builder} that can be used to build up a
 * set of bindings incrementally. Additionally, bindings can be combined using {@link #mergedWith(PlannerBindings)}.
 */
@API(API.Status.EXPERIMENTAL)
public class PlannerBindings {
    @Nonnull
    private static final PlannerBindings EMPTY = new PlannerBindings(ImmutableListMultimap.of());

    @Nonnull
    private final ImmutableListMultimap<BindingMatcher<?>, ?> bindings;

    private PlannerBindings(@Nonnull ImmutableListMultimap<BindingMatcher<?>, ?> bindings) {
        this.bindings = bindings;
    }

    /**
     * Checks whether there is an object bound to {@code key}.
     * @param key a matcher
     * @return whether there is an object bound to {@code key}
     */
    public boolean containsKey(@Nonnull BindingMatcher<?> key) {
        return bindings.containsKey(key);
    }

    /**
     * Retrieve the single bindable bound to {@code key}. This method is meant to be a convenient shortcut for the case
     * where the developer is certain that precisely one bindable could be bound to this key. If no bindable is bound to
     * this key, or if there are multiple objects bound to this key, this throws a {@link NoSuchElementException}.
     * @param key a matcher
     * @param <T> the type of objects that was bound to {@code key}
     * @return the bindable object bound to key
     * @throws NoSuchElementException if the number of bindables bound to this key is not exactly one
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull BindingMatcher<T> key) {
        if (bindings.containsKey(key)) {
            List<?> bindingsForKey = bindings.get(key);
            if (bindingsForKey.size() == 1) {
                return (T)bindingsForKey.get(0);
            } else {
                throw new NoSuchElementException("attempted to retrieve individual bindable but multiple keys were bound");
            }
        }
        throw new NoSuchElementException("attempted to extract bindable from binding using non-existent key");
    }

    /**
     * Retrieve all objects bound to {@code key} if there is at least one such bindable. The bindables in the returned
     * list appear in same order as they appear in the list of children of the {@link RelationalExpression}
     * that produced this set of bindings. If no bindable is bound to this key, throw a {@link NoSuchElementException}.
     * @param key a matcher
     * @param <T> the type of objects that was bound to {@code key}
     * @return a list of bindable objects bound to the key
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> List<T> getAll(@Nonnull BindingMatcher<T> key) {
        if (bindings.containsKey(key)) {
            return (List<T>)bindings.get(key);
        }
        return ImmutableList.of();
    }

    /**
     * Combine this set of bindings with the given set of bindings. If both sets of bindings contain a binding with the
     * same key, the resulting set of bindings will contain the <em>concatenation</em> of the bindables from this set
     * of bindings and the bindables from the given set of bindings, in that order, providing a predictable ordering
     * of bindings for planner rules that might need it.
     * @param other a set of bindings, which may share keys with this set of bindings
     * @return a new set of bindings that contains the bindings from both both sets of bindings
     */
    @Nonnull
    public PlannerBindings mergedWith(@Nonnull PlannerBindings other) {
        ImmutableListMultimap.Builder<BindingMatcher<?>, Object> combined = ImmutableListMultimap.builder();
        combined.putAll(this.bindings);
        combined.putAll(other.bindings);
        return new PlannerBindings(combined.build());
    }

    /**
     * Return the bindings as multimap for callers that need reason about all bindings in a generic manner, e.g. this
     * can be used by implementations of {@link com.apple.foundationdb.record.query.plan.cascades.debug.Debugger} to
     * show the contents of all bindings without the knowledge of the existence of particular matchers.
     * @return the backing immutable multi map
     */
    @Nonnull
    public ImmutableListMultimap<BindingMatcher<?>, ?> asMultiMap() {
        return bindings;
    }

    @Override
    public String toString() {
        return bindings.toString();
    }

    /**
     * Build a new set of bindings containing a single binding from the given key to the given bindable.
     * @param key an expression matcher
     * @param object an object
     * @param <T> type of object
     * @return a new set of bindings containing a single binding from {@code key} to {@code bindable}
     */
    @Nonnull
    public static <T> PlannerBindings from(@Nonnull BindingMatcher<T> key, @Nonnull T object) {
        return new PlannerBindings(ImmutableListMultimap.of(key, object));
    }

    /**
     * Return an empty set of bindings.
     * @return an empty set of bindings
     */
    @Nonnull
    public static PlannerBindings empty() {
        return EMPTY;
    }

    /**
     * Return a new builder.
     * @return a new builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A mutable builder for a set of {@link PlannerBindings} which can be used to incrementally build up a set of
     * bindings without repeatedly calling {@link #mergedWith(PlannerBindings)}, which is less efficient.
     */
    public static class Builder {
        @Nonnull
        private final ImmutableListMultimap.Builder<BindingMatcher<?>, Object> map;

        public Builder() {
            this.map = ImmutableListMultimap.builder();
        }

        @Nonnull
        public <T> Builder put(@Nonnull BindingMatcher<? extends T> key, @Nonnull T bindable) {
            map.put(key, bindable);
            return this;
        }

        @Nonnull
        public <T> Builder putAll(@Nonnull BindingMatcher<? extends T> key, @Nonnull Iterable<? extends T> bindables) {
            map.putAll(key, bindables);
            return this;
        }

        @Nonnull
        public Builder putAll(@Nonnull PlannerBindings plannerBindings) {
            map.putAll(plannerBindings.bindings);
            return this;
        }

        public PlannerBindings build() {
            return new PlannerBindings(map.build());
        }
    }
}
