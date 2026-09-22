/*
 * IndexPredicates.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.query.plan.cascades.IndexPredicateExpansion;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Locale;

import static java.util.stream.Collectors.toList;

/**
 * The form a predicate takes to be stored with an index.
 */
final class IndexPredicates {

    private IndexPredicates() {
    }

    /**
     * The residual conjunction of the select's predicates, normalised to DNF and checked to be one the deserializer
     * supports. Falls back to the un-normalised conjunction when the DNF cannot be expressed as ranges.
     *
     * @param predicates the predicates the select carries, at least one
     *
     * @return the predicate to store
     */
    @Nonnull
    static QueryPredicate normalize(@Nonnull final List<? extends QueryPredicate> predicates) {
        final var residuals = predicates.stream().map(QueryPredicate::toResidualPredicate).collect(toList());
        final var conjunction = residuals.size() == 1 ? residuals.get(0) : AndPredicate.and(residuals);
        final var normalized = BooleanPredicateNormalizer.getDefaultInstanceForDnf()
                .normalize(conjunction, false).orElse(conjunction);
        Assert.thatUnchecked(IndexPredicate.isSupported(normalized), ErrorCode.UNSUPPORTED_OPERATION,
                () -> String.format(Locale.ROOT, "Unsupported predicate '%s'", normalized));
        return IndexPredicateExpansion.dnfPredicateToRanges(normalized).isEmpty() ? conjunction : normalized;
    }
}
