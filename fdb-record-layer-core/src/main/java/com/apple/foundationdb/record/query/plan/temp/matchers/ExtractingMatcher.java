/*
 * ExtractingMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * TODO.
 * @param <T>
 */
public interface ExtractingMatcher<T> {
    @Nonnull
    Stream<PlannerBindings> unapplyAndBindExtractedMatches(@Nonnull PlannerBindings outerBindings, @Nonnull T in);

    @Nonnull
    static <T, U> ExtractingMatcher<T> of(@Nonnull final Extractor<T, U> extractor, @Nonnull final BindingMatcher<?> matcher) {
        return new ExtractingMatcher<T>() {
            @Nonnull
            @Override
            public Stream<PlannerBindings> unapplyAndBindExtractedMatches(@Nonnull final PlannerBindings outerBindings, @Nonnull final T in) {
                return matcher.bindMatches(outerBindings, extractor.unapply(in));
            }
        };
    }
}
