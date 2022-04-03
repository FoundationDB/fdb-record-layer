/*
 * AndComponent.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.QueryHashable;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A {@link QueryComponent} that is satisfied when all of its child components are satisfied.
 *
 * For tri-valued logic:
 * <ul>
 * <li>If all children are {@code true}, then {@code true}.</li>
 * <li>If any child is {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
@API(API.Status.MAINTAINED)
public class AndComponent extends AndOrComponent {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("And-Component");

    public AndComponent(@Nonnull List<QueryComponent> operands) {
        super(operands);
    }

    @Nonnull
    public static AndComponent from(@Nonnull List<? extends QueryComponent> operands) {
        ImmutableList.Builder<QueryComponent> operandRefs = ImmutableList.builder();
        for (QueryComponent operand : operands) {
            operandRefs.add(operand);
        }
        return new AndComponent(operandRefs.build());
    }

    @Override
    public boolean isOr() {
        return false;
    }

    @Override
    public String toString() {
        return "And(" + getChildren() + ")";
    }

    @Override
    public QueryComponent withOtherChildren(List<QueryComponent> newChildren) {
        return AndComponent.from(newChildren);
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nonnull final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {
        return GraphExpansion.ofOthers(getChildren().stream()
                .map(child -> child.expand(baseQuantifier, outerQuantifierSupplier, fieldNamePrefix))
                .collect(ImmutableList.toImmutableList()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }
        AndComponent that = (AndComponent) o;
        return Objects.equals(getChildren(), that.getChildren());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChildren());
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return PlanHashable.planHash(hashKind, getChildren());
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChildren());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getChildren());
    }
}
