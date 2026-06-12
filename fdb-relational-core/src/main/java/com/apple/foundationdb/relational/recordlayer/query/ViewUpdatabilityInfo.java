/*
 * ViewUpdatabilityInfo.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Carries the information extracted from an updatable view's compiled plan that DML visitors need to
 * rewrite INSERT/UPDATE/DELETE targeting the view into equivalent DML against the base table.
 */
@API(API.Status.EXPERIMENTAL)
public final class ViewUpdatabilityInfo {

    @Nonnull
    private final RecordLayerTable baseTable;

    @Nonnull
    private final Identifier baseTableIdentifier;

    /**
     * Predicates from the view's SelectExpression (the view's own WHERE clause, if any).
     * These reference the view's inner quantifier alias ({@link #viewInnerAlias}).
     */
    @Nonnull
    private final List<? extends QueryPredicate> viewPredicates;

    /**
     * The alias of the quantifier that flows through the view's SelectExpression. View predicates
     * reference this alias and must be translated before use in a different quantifier context.
     */
    @Nonnull
    private final CorrelationIdentifier viewInnerAlias;

    public ViewUpdatabilityInfo(@Nonnull final RecordLayerTable baseTable,
                                @Nonnull final Identifier baseTableIdentifier,
                                @Nonnull final List<? extends QueryPredicate> viewPredicates,
                                @Nonnull final CorrelationIdentifier viewInnerAlias) {
        this.baseTable = baseTable;
        this.baseTableIdentifier = baseTableIdentifier;
        this.viewPredicates = viewPredicates;
        this.viewInnerAlias = viewInnerAlias;
    }

    @Nonnull
    public RecordLayerTable baseTable() {
        return baseTable;
    }

    @Nonnull
    public Identifier baseTableIdentifier() {
        return baseTableIdentifier;
    }

    @Nonnull
    public List<? extends QueryPredicate> viewPredicates() {
        return viewPredicates;
    }

    @Nonnull
    public CorrelationIdentifier viewInnerAlias() {
        return viewInnerAlias;
    }
}
