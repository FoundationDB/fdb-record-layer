/*
 * OrComponent.java
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
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A {@link QueryComponent} that is satisfied when any of its child components is satisfied.
 *
 * For tri-valued logic:
 * <ul>
 * <li>If any child is {@code true}, then {@code true}.</li>
 * <li>If all children are {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
@API(API.Status.MAINTAINED)
public class OrComponent extends AndOrComponent {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Or-Component");

    public OrComponent(List<QueryComponent> operands) {
        super(operands);
    }

    public static OrComponent from(@Nonnull List<QueryComponent> operands) {
        return new OrComponent(operands);
    }

    @Override
    public boolean isOr() {
        return true;
    }

    @Override
    public String toString() {
        return "Or(" + getChildren() + ")";
    }

    @Override
    public QueryComponent withOtherChildren(List<QueryComponent> newChildren) {
        return OrComponent.from(newChildren);
    }

    @Override
    public GraphExpansion expand(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        final GraphExpansion childrenGraphExpansion =
                GraphExpansion.ofOthers(getChildren().stream()
                        .map(child -> child.expand(baseAlias, fieldNamePrefix))
                        .map(expanded -> expanded.withPredicate(expanded.asAndPredicate()))
                        .collect(ImmutableList.toImmutableList()));

        return childrenGraphExpansion.withPredicate(OrPredicate.or(childrenGraphExpansion.getPredicates()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }
        OrComponent that = (OrComponent) o;
        return Objects.equals(getChildren(), that.getChildren());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
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
}
