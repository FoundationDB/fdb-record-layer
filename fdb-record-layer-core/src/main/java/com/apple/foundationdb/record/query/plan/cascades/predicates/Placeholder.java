/*
 * Placeholder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Placeholder is basically a {@link ValueWithRanges} with an alias that is used solely used for index matching.
 */
public class Placeholder extends ValueWithRanges implements WithAlias {

    @Nonnull
    private final CorrelationIdentifier alias;

    private Placeholder(@Nonnull Value value,
                       @Nonnull final Set<RangeConstraints> rangeConstraints,
                       @Nonnull final CorrelationIdentifier alias) {
        super(value, rangeConstraints);
        this.alias = alias;
    }

    @Override
    public boolean isSargable() {
        return false;
    }

    @Nonnull
    public static Placeholder newInstance(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, Set.of(), parameterAlias);
    }

    @Nonnull
    public Placeholder withExtraRanges(@Nonnull final Set<RangeConstraints> ranges) {
        return new Placeholder(getValue(), Stream.concat(ranges.stream(), getRanges().stream()).collect(Collectors.toSet()), getAlias());
    }

    @Nonnull
    @Override
    public Placeholder translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return new Placeholder(getValue().translateCorrelations(translationMap), getRanges().stream().map(range -> range.translateCorrelations(translationMap)).collect(Collectors.toSet()), getAlias());
    }

    @Nonnull
    @Override
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return super.toString() + " -> " + getAlias();
    }
}
