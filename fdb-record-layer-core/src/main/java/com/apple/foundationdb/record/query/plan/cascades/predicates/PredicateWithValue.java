/*
 * PredicateWithValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.WithValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;

/**
 * A predicate consisting of a {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
public interface PredicateWithValue extends LeafQueryPredicate, WithValue<PredicateWithValue> {

    /**
     * Replaces the {@link Value} and its associated {@code Value}(s) within the {@code Comparison}.
     * To avoid unnecessary object creation, this method expects the caller to return the original objects
     * if no translation is required.
     *
     * @param valueTranslator the value translator.
     * @param comparisonTranslator the comparison translator.
     *
     * @return potentially a new instance of {@code PredicateWithValue} with translated {@code Value}(s).
     */
    @Nonnull
    Optional<? extends PredicateWithValue> translateValueAndComparisonsMaybe(@Nonnull Function<Value, Optional<Value>> valueTranslator,
                                                                             @Nonnull Function<Comparisons.Comparison, Optional<Comparisons.Comparison>> comparisonTranslator);
}
