/*
 * TestExpression.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import edu.umd.cs.findbugs.annotations.NonNull;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of {@link RelationalExpression} that is useful for testing. It has a stable
 * hash code, so it can be used with the {@link PlanningCostModel#planHashTiebreaker()}, but
 * all of its other characteristics are not set to anything interesting.
 */
class TestExpressionWithFixedSemanticHash extends AbstractRelationalExpression {
    @Nonnull
    private final String name;
    private final int hashCode;

    public TestExpressionWithFixedSemanticHash(@Nonnull String name, int hashCode) {
        this.name = name;
        this.hashCode = hashCode;
    }

    @NonNull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return hashCode;
    }

    @Override
    public int semanticHashCode() {
        return hashCode;
    }

    @NonNull
    @Override
    public Value getResultValue() {
        return LiteralValue.ofScalar(42L);
    }

    @NonNull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return List.of();
    }

    @Override
    public boolean equalsWithoutChildren(@NonNull final RelationalExpression other, @NonNull final AliasMap equivalences) {
        if (other instanceof TestExpressionWithFixedSemanticHash) {
            return hashCode == other.semanticHashCode() && Objects.equals(name, ((TestExpressionWithFixedSemanticHash)other).getName());
        }
        return false;
    }

    @NonNull
    @Override
    public RelationalExpression translateCorrelations(@NonNull final TranslationMap translationMap, final boolean shouldSimplifyValues, @NonNull final List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @NonNull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return Set.of();
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "test(name=" + name + ", hash=" + hashCode + ")";
    }
}
