/*
 * InstanceMatcher.java
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.base.Verify;

import java.util.stream.Stream;

/**
 * A binding matcher that matches the same object as another {@link BindingMatcher}.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class InstanceMatcher<T> implements BindingMatcher<T> {
    private final BindingMatcher<T> otherMatcher;

    public InstanceMatcher(final BindingMatcher<T> otherMatcher) {
        this.otherMatcher = otherMatcher;
    }

    @Override
    public Class<T> getRootClass() {
        return otherMatcher.getRootClass();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Stream<PlannerBindings> bindMatchesSafely(RecordQueryPlannerConfiguration plannerConfiguration, PlannerBindings outerBindings, T in) {
        Verify.verify(outerBindings.containsKey(otherMatcher));

        if (outerBindings.get(otherMatcher) == in) {
            return Stream.of(PlannerBindings.from(this, in));
        } else {
            return Stream.empty();
        }
    }

    @Override
    public String explainMatcher(final Class<?> atLeastType, final String boundId, final String indentation) {
        return "case _: " + getRootClass().getSimpleName() + " if " + boundId + " is bound in other matcher => success ";
    }

    public static <T> BindingMatcher<T> sameInstanceAsBound(final BindingMatcher<T> otherMatcher) {
        return new InstanceMatcher<>(otherMatcher);
    }
}
