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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.apple.foundationdb.record.query.predicates.OrPredicate;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    public ExpandedPredicates normalizeForPlanner(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        final ExpandedPredicates childrenExpandedPredicates =
                ExpandedPredicates.fromOthers(getChildren().stream()
                        .map(child -> child.normalizeForPlanner(baseAlias, fieldNamePrefix))
                        .map(expanded -> ExpandedPredicates.fromOtherWithPredicate(expanded.asAndPredicate(), expanded.getQuantifiers()))
                        .collect(Collectors.toList()));

        return ExpandedPredicates.fromOtherWithPredicate(OrPredicate.or(childrenExpandedPredicates.getPredicates()),
                childrenExpandedPredicates.getQuantifiers());
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
    public int planHash() {
        return PlanHashable.planHash(getChildren());
    }
}
