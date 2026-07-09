/*
 * NamedPredicate.java
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

package com.apple.foundationdb.record.query.plan.match;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

/**
 * Matcher that applies a {@link Predicate} and has a name as its description.
 * @param <T> type of argument to the predicate
 */
public class NamedPredicate<T> extends TypeSafeMatcher<T> {
    @Nonnull
    private final Predicate<T> inner;

    private final String name;

    public NamedPredicate(@Nonnull Predicate<T> inner) {
        this(inner, null);
    }

    public NamedPredicate(@Nonnull Predicate<T> inner, String name) {
        this.inner = inner;
        this.name = name;
    }

    @Nonnull
    public Predicate<T> getInner() {
        return inner;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean matchesSafely(T val) {
        return inner.test(val);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(getName());
    }

    @Override
    public String toString() {
        return getName();
    }
}
