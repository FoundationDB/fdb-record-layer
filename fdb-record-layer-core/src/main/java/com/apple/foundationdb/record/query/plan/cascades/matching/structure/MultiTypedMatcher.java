/*
 * MultiTypedMatcher.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link TypedMatcher} that matches a fixed set of concrete subclasses of a common supertype. The matcher advertises
 * the concrete subclasses via {@link #getRootClasses()} and relies on the indexing mechanism in the rule sets
 * to effectively do the filtering by subclass.
 *
 * @param <T> the common supertype that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class MultiTypedMatcher<T> extends TypedMatcher<T> {

    protected MultiTypedMatcher(@Nonnull final Class<T> commonSuperClass,
                                @Nonnull final Set<? extends Class<?>> rootClasses) {
        super(commonSuperClass, rootClasses);
        Verify.verify(!rootClasses.isEmpty());
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                                     @Nonnull final PlannerBindings outerBindings,
                                                     @Nonnull final T in) {
        // Sanity-check that the `in` class is one of the concrete subclasses. This matcher does not actively filter
        // classes itself and instead relies on the rule-set indexing path to do this.
        Debugger.sanityCheck(() -> Verify.verify(getRootClasses().contains(in.getClass()),
                "MultiTypedMatcher invoked with %s; expected one of %s", in.getClass(), getRootClasses()));
        return super.bindMatchesSafely(plannerConfiguration, outerBindings, in);
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId,
                                 @Nonnull final String indentation) {
        final String alternatives = getRootClasses().stream()
                .map(Class::getSimpleName)
                .collect(Collectors.joining(" | "));
        return "case _: (" + alternatives + ") => success ";
    }
}
