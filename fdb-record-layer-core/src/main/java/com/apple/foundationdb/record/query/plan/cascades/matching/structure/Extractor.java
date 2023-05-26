/*
 * Extractor.java
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

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;

import javax.annotation.Nonnull;
import java.util.function.UnaryOperator;

/**
 * Interface for the unapply function using in a variety of matchers.
 * @param <T> the type that is extracted from
 * @param <U> the type that is extracted into
 */
public class Extractor<T, U> implements UnapplyWithConfiguration<T, U> {
    @Nonnull
    private final UnapplyWithConfiguration<T, U> unapplyFn;
    @Nonnull
    private final UnaryOperator<String> explainFn;

    public Extractor(@Nonnull final Unapply<T, U> unapplyFn, @Nonnull final UnaryOperator<String> explainFn) {
        this((plannerConfiguration, t) -> unapplyFn.unapply(t), explainFn);
    }

    public Extractor(@Nonnull final UnapplyWithConfiguration<T, U> unapplyFn, @Nonnull final UnaryOperator<String> explainFn) {
        this.unapplyFn = unapplyFn;
        this.explainFn = explainFn;
    }

    @Override
    @Nonnull
    public U unapply(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration, @Nonnull final T t) {
        return unapplyFn.unapply(plannerConfiguration, t);
    }

    @Nonnull
    public String explainExtraction(@Nonnull final String name) {
        return explainFn.apply(name);
    }

    public static <T> Extractor<T, T> identity() {
        return new Extractor<>(t -> t, name -> name);
    }

    public static <T, U> Extractor<T, U> of(@Nonnull final Unapply<T, U> unapply, final UnaryOperator<String> explainFn) {
        return new Extractor<>(unapply, explainFn);
    }

    public static <T, U> Extractor<T, U> of(@Nonnull final UnapplyWithConfiguration<T, U> unapply, final UnaryOperator<String> explainFn) {
        return new Extractor<>(unapply, explainFn);
    }
}
