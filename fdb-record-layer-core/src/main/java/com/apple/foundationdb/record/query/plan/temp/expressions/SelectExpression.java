/*
 * SelectExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A select expression.
 */
@API(API.Status.EXPERIMENTAL)
public class SelectExpression implements RelationalExpressionWithChildren, RelationalExpressionWithPredicate {
    @Nonnull
    private final List<Quantifier> children;
    @Nonnull
    private final List<QueryPredicate> predicates;

    public SelectExpression(@Nonnull List<Quantifier> children) {
        this(children, Collections.singletonList(ConstantPredicate.TRUE));
    }

    public SelectExpression(@Nonnull List<Quantifier> children, @Nonnull List<QueryPredicate> predicates) {
        this.children = children;
        this.predicates = predicates;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return children;
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }

    @Override
    @Nonnull
    public QueryPredicate getPredicate() {
        return new AndPredicate(predicates);
    }

    @Nonnull
    public List<QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return getPredicate().getCorrelatedTo();
    }

    @Nonnull
    @Override
    public SelectExpression rebase(@Nonnull final AliasMap translationMap) {
        return (SelectExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public SelectExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        List<QueryPredicate> rebasedPredicates = predicates.stream().map(p -> p.rebase(translationMap)).collect(Collectors.toList());
        return new SelectExpression(rebasedQuantifiers, rebasedPredicates);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return getPredicate().semanticEquals(((SelectExpression)otherExpression).getPredicate(), equivalencesMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getPredicate());
    }

    public static class Builder {
        @Nonnull
        private final List<Quantifier> children;
        @Nonnull
        private final List<QueryPredicate> predicates;

        public Builder(@Nonnull Quantifier firstChild) {
            this(Lists.newArrayList(firstChild), new ArrayList<>());
        }

        public Builder(@Nonnull List<Quantifier> children, @Nonnull List<QueryPredicate> predicates) {
            this.children = children;
            this.predicates = predicates;
        }

        @Nonnull
        public Builder addChild(@Nonnull Quantifier quantifier) {
            children.add(quantifier);
            return this;
        }

        @Nonnull
        public Builder addPredicate(@Nonnull QueryPredicate predicate) {
            predicates.add(predicate);
            return this;
        }

        @Nonnull
        public CorrelationIdentifier getCorrelationBase() {
            return children.get(0).getAlias();
        }

        @Nonnull
        public Builder copy() {
            return new Builder(new ArrayList<>(children), new ArrayList<>(predicates));
        }

        @Nonnull
        public SelectExpression build() {
            return new SelectExpression(children, predicates);
        }
    }
}
