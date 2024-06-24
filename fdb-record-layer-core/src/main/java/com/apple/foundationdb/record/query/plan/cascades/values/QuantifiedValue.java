/*
 * QuantifiedValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * A scalar value type that is directly derived from an alias.
 */
@API(API.Status.EXPERIMENTAL)
public interface QuantifiedValue extends LeafValue {

    @Nonnull
    CorrelationIdentifier getAlias();

    @Nonnull
    @Override
    default Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(getAlias());
    }

    @Nonnull
    @Override
    default Optional<QueryPlanConstraint> equalsWithoutChildren(@Nonnull final Value other) {
        final var superQueryPlanConstraint = LeafValue.super.equalsWithoutChildren(other);
        if (superQueryPlanConstraint.isEmpty()) {
            return Optional.empty();
        }

        final QuantifiedValue that = (QuantifiedValue)other;
        return getAlias().equals(that.getAlias()) ? superQueryPlanConstraint : Optional.empty();
    }
}
