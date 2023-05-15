/*
 * TypedMatcherWithExtractAndDownstream.java
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

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher.newLine;

/**
 * A {@link TypedMatcher} that also extracts an object from the current object and attempts to match that object with
 * a {@code downstream} matcher.
 * @param <T> the type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TypedMatcherWithExtractAndDownstream<T> extends TypedMatcher<T> {
    @Nonnull
    private final Extractor<? super T, ?> extractor;
    @Nonnull
    private final BindingMatcher<?> downstream;

    protected TypedMatcherWithExtractAndDownstream(@Nonnull final Class<T> bindableClass,
                                                   @Nonnull final Extractor<? super T, ?> extractor,
                                                   @Nonnull final BindingMatcher<?> downstream) {
        super(bindableClass);
        this.extractor = extractor;
        this.downstream = downstream;
    }

    @Nonnull
    public Extractor<? super T, ?> getExtractor() {
        return extractor;
    }

    @Nonnull
    public BindingMatcher<?> getDownstream() {
        return downstream;
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
        return super.bindMatchesSafely(plannerConfiguration, outerBindings, in)
                .flatMap(bindings ->
                        downstream
                                .bindMatches(plannerConfiguration, outerBindings, extractor.unapply(plannerConfiguration, in))
                                .map(bindings::mergedWith));
    }

    @Override
    public String explainMatcher(@Nonnull final Class<?> atLeastType, @Nonnull final String boundId, @Nonnull final String indentation) {
        final String nestedId = downstream.identifierFromMatcher();
        final String nestedIndentation = indentation + INDENTATION;
        final String doubleNestedIndentation = nestedIndentation + INDENTATION;

        final String typeConstraint =
                getRootClass().isAssignableFrom(atLeastType)
                ? ""
                : " if " + boundId + " instanceOf[" + getRootClass().getSimpleName() + "]";

        return boundId + " match { " + newLine(nestedIndentation) +
               "case " + extractor.explainExtraction(nestedId) + typeConstraint + " => " + newLine(doubleNestedIndentation) +
               downstream.explainMatcher(downstream.unboundRootClass(), nestedId, doubleNestedIndentation) + newLine(indentation) +
               "}";
    }

    @Nonnull
    public static <S, T extends S> TypedMatcherWithExtractAndDownstream<T> typedWithDownstream(@Nonnull final Class<T> bindableClass,
                                                                                               @Nonnull final Extractor<? super T, ?> extractor,
                                                                                               @Nonnull final BindingMatcher<?> downstream) {
        return new TypedMatcherWithExtractAndDownstream<>(bindableClass, extractor, downstream);
    }
}
