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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

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

    public AndComponent(@Nonnull List<ExpressionRef<QueryComponent>> operands) {
        super(operands);
    }

    public static AndComponent from(@Nonnull List<? extends QueryComponent> operands) {
        ImmutableList.Builder<ExpressionRef<QueryComponent>> operandRefs = ImmutableList.builder();
        for (QueryComponent operand : operands) {
            operandRefs.add(SingleExpressionRef.of(operand));
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
    public int planHash() {
        return PlanHashable.planHash(getChildren());
    }
}
