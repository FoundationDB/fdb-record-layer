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

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * A binding matcher that matches the same object as another {@link BindingMatcher}.
 *
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class InstanceMatcher<T> implements BindingMatcher<T> {
    @Nonnull
    private final BindingMatcher<T> otherMatcher;

    public InstanceMatcher(@Nonnull final BindingMatcher<T> otherMatcher) {
        this.otherMatcher = otherMatcher;
    }

    @Nonnull
    @Override
    public Class<T> getRootClass() {
        return otherMatcher.getRootClass();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull PlannerBindings outerBindings, @Nonnull T in) {
        Verify.verify(outerBindings.containsKey(otherMatcher));

        if (outerBindings.get(otherMatcher) == in) {
            return Stream.of(PlannerBindings.from(this, in));
        } else {
            return Stream.empty();
        }
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        return "case _: " + getRootClass().getSimpleName() + " if " + boundId + " is bound in other matcher => success ";
    }

    @Nonnull
    public static <T> BindingMatcher<T> sameInstanceAsBound(@Nonnull final BindingMatcher<T> otherMatcher) {
        return new InstanceMatcher<>(otherMatcher);
    }
}
