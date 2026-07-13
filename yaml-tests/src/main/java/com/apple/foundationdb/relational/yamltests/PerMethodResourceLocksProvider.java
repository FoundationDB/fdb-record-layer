/*
 * PerMethodResourceLocksProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLocksProvider;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * A {@link ResourceLocksProvider} that hands each {@code @YamlTest} test method its own unique
 * lock keyed by the method's fully-qualified name.
 *
 * <p>The point is to let JUnit run different {@code @TestTemplate} methods in {@code @YamlTest}
 * classes in parallel with each other while still serialising the multiple
 * {@link org.junit.jupiter.api.TestTemplate} invocations <em>within a single method</em>. Each
 * yamsql file typically hard-codes cluster-wide state (databases, schema templates, plan-cache
 * seeds, planner-metric expectations) that the framework relies on being touched by only one
 * runner configuration at a time — so two invocations of the same method against the same
 * cluster can't safely run in parallel, but two <em>different</em> methods against
 * <em>different</em> yamsql files (with disjoint hard-coded names) can.
 *
 * <p>Wiring: {@code @YamlTest} is meta-annotated with
 * {@code @ResourceLock(providers = PerMethodResourceLocksProvider.class)}. JUnit invokes
 * {@link #provideForMethod} once per test method, and the returned lock is applied uniformly
 * to every {@link org.junit.jupiter.api.TestTemplate} invocation of that method — so
 * invocations of one method compete for the shared key and run one at a time, while invocations
 * of different methods hold different keys and can proceed concurrently.
 */
public class PerMethodResourceLocksProvider implements ResourceLocksProvider {

    private static final String KEY_PREFIX = "yaml-test/";

    @Override
    public Set<Lock> provideForMethod(List<Class<?>> enclosingInstanceTypes, Class<?> testClass, Method testMethod) {
        // The key uses the fully-qualified class name so that if two @YamlTest classes happen to
        // declare a method with the same simple name they still get independent locks.
        final String key = KEY_PREFIX + testClass.getName() + "#" + testMethod.getName();
        return Set.of(new Lock(key, ResourceAccessMode.READ_WRITE));
    }
}
