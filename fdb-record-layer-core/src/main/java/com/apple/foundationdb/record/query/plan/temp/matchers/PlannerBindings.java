/*
 * PlannerBindings.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * <code>PlannerBindings</code> is a map-like structure that supports a map from a binding to both {@link PlannerExpression}s
 * and {@link ExpressionRef}s, enforcing that a single name may map to only one binding of either type.
 * A binding keys is a pointer to the {@link ExpressionMatcher} that created the binding, eliminating the need for a
 * unique string or symbol identifier.
 */
public class PlannerBindings {
    @Nonnull
    private final Map<ExpressionMatcher<? extends Bindable>, Bindable> bindings = new HashMap<>();

    /**
     * Checks whether there is a bindable bound to <code>key</code>.
     * @param key a matcher
     * @return whether there is an object bound to <code>key</code>
     */
    public boolean containsKey(@Nonnull ExpressionMatcher<? extends Bindable> key) {
        return bindings.containsKey(key);
    }

    /**
     * Retrieve the object bound to <code>key</code>.
     * @param key a matcher
     * @param <T> the type of {@link Bindable} that was bound to <code>key</code>
     * @return the bindable object bound to key
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T extends Bindable> T get(@Nonnull ExpressionMatcher<T> key) {
        if (bindings.containsKey(key)) {
            return (T) bindings.get(key);
        }
        throw new NoSuchElementException("attempted to extract bindable from binding using non-existent key");
    }

    /**
     * Bind the given key to the provided {@link Bindable}. Note that <code>bindable</code> must be an instance
     * of the type parameter of <code>key</code>, although the compiler cannot enforce this because of how it is used
     * in {@link Bindable#bindWithExisting(ExpressionMatcher, PlannerBindings)}.
     * @param key a matcher with type parameter that is a super class of the class of <code>bindable</code>
     * @param bindable a bindable object to bind to the key
     */
    public void put(@Nonnull ExpressionMatcher<? extends Bindable> key, @Nonnull Bindable bindable) {
        if (bindings.containsKey(key)) {
            throw new RecordCoreException("attempted to add a binding that already exists");
        }
        bindings.put(key, bindable);
    }
}
