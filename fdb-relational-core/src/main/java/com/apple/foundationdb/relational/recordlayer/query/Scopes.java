/*
 * Scopes.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A tree of {@link Scope}s.
 */
public class Scopes {
    @Nullable
    private Scope currentScope;

    public Scopes() {
        this(null);
    }

    public Scopes(@Nullable Scope currentScope) {
        this.currentScope = currentScope;
    }

    @Nullable
    public Scope getCurrentScope() {
        return currentScope;
    }

    @Nonnull
    public Scope push() {
        this.currentScope = Scope.withParent(currentScope);
        return currentScope;
    }

    @Nonnull
    public Scope sibling() {
        this.currentScope = Scope.withParentAndSibling(currentScope == null ? null : currentScope.parent, currentScope);
        return currentScope;
    }

    @Nonnull
    public Scope pop() {
        Assert.notNullUnchecked(currentScope);
        try {
            return currentScope;
        } finally {
            currentScope = currentScope.sibling == null ? currentScope.parent : currentScope.sibling;
        }
    }

    public <T> T withChildScope(@Nonnull Function<Scope, T> supplier) {
        final var currentScope = push();
        try {
            return supplier.apply(currentScope);
        } finally {
            pop();
        }
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final String identifier, boolean lookIntoSiblings) {
        return resolveQuantifier(CorrelationIdentifier.of(identifier), lookIntoSiblings);
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final CorrelationIdentifier identifier, boolean lookIntoSiblings) {
        Scope scope = currentScope;
        while (scope != null)  {
            final var maybeQuantifier = scope.getQuantifier(identifier);
            if (maybeQuantifier.isPresent()) {
                return maybeQuantifier;
            }
            if (lookIntoSiblings && scope.sibling != null) {
                scope = scope.sibling;
            } else {
                scope = scope.getParent();
            }
        }
        return Optional.empty();
    }

    /**
     * A frame of the scopes stack, binding names to quantifiers.
     */
    public static final class Scope {

        @Nullable
        private CorrelationIdentifier groupByQuantifierCorrelation;

        /**
         * Set of flags to control the behavior of parser.
         */
        public enum Flag { UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE, RESOLVING_AGGREGATION, RESOLVING_SELECT_HAVING }

        @Nullable
        private final Scope parent;

        @Nullable
        private final Scope sibling;

        @Nonnull
        private final Map<CorrelationIdentifier, Quantifier> quantifiers;

        @Nonnull
        private final List<Column<? extends Value>> selectedColumnList;

        @Nonnull
        private final List<Integer> projectedCardinals;

        @Nonnull
        private final List<Integer> orderByCardinals;

        private boolean isReverse;

        @Nullable
        private QueryPredicate predicate;

        @Nonnull
        private final EnumSet<Flag> flags;

        private int aggCounter;

        @Nullable
        private Type groupByType;

        @Nonnull
        private List<AggregateValue> aggregateValues;

        @Nonnull
        private List<Integer> aggregateReferences;

        @Nullable
        private Type targetType;

        @Nullable
        private List<String> targetTypeReorderings;

        private Scope(@Nullable final Scope parent,
                      @Nullable final Scope sibling,
                      @Nonnull final Map<CorrelationIdentifier, Quantifier> quantifiers,
                      @Nonnull final List<Column<? extends Value>> selectedColumnList,
                      @Nullable final QueryPredicate predicate) {
            this.parent = parent;
            this.sibling = sibling;
            this.quantifiers = quantifiers;
            this.selectedColumnList = selectedColumnList;
            this.predicate = predicate;
            this.flags = EnumSet.noneOf(Flag.class);
            this.aggCounter = 0;
            this.groupByQuantifierCorrelation = null;
            this.groupByType = null;
            this.aggregateValues = new ArrayList<>();
            this.aggregateReferences = new ArrayList<>();
            this.projectedCardinals = new ArrayList<>();
            this.orderByCardinals = new ArrayList<>();
        }

        @Nonnull
        private SelectExpression convertToSelectExpression() {
            final GraphExpansion.Builder builder = GraphExpansion.builder();
            builder.addAllQuantifiers(new ArrayList<>(quantifiers.values()))
                    .addAllResultColumns(selectedColumnList);
            if (predicate != null) {
                builder.addPredicate(predicate);
            }
            return builder.build().buildSelect();
        }

        @Nonnull
        private LogicalSortExpression convertToLogicalSortExpression() {
            SelectExpression selectExpression = convertToSelectExpression();
            final var qun = Quantifier.forEach(GroupExpressionRef.of(selectExpression));
            final var orderByValues = orderByCardinals.stream()
                    .map(i -> FieldValue.ofOrdinalNumber(qun.getFlowedObjectValue(), i))
                    .collect(Collectors.toList());
            final var aliasMap = AliasMap.of(qun.getAlias(), Quantifier.current());
            final var rebasedOrderByValues = orderByValues.stream().map(val -> val.rebase(aliasMap)).collect(Collectors.toList());
            return new LogicalSortExpression(rebasedOrderByValues, isReverse, qun);
        }

        @Nonnull
        public RelationalExpression convertToRelationalExpression() {
            RelationalExpression relationalExpression;
            if (getParent() != null || getSibling() != null) {
                Assert.thatUnchecked(orderByCardinals.isEmpty(), "ORDER BY is only supported for top level selects", ErrorCode.UNSUPPORTED_OPERATION);
                relationalExpression = convertToSelectExpression();
            } else {
                relationalExpression = convertToLogicalSortExpression();
            }
            if (projectedCardinals.size() != selectedColumnList.size()) {
                final var qun = Quantifier.forEach(GroupExpressionRef.of(relationalExpression));
                final var builder = GraphExpansion.builder().addQuantifier(qun);
                for (int idx : projectedCardinals) {
                    builder.addResultColumn(qun.getFlowedColumns().get(idx));
                }
                return builder.build().buildSelect();
            }
            return relationalExpression;
        }

        public void addQuantifier(@Nonnull final Quantifier quantifier) {
            if (hasQuantifier(quantifier.getAlias())) {
                // TODO we should use error codes for proper dispatch in caller.
                throw new SemanticException(SemanticException.ErrorCode.UNKNOWN,
                        String.format("quantifier with name '%s' already exists in scope", quantifier.getAlias()),
                        null);
            }
            quantifiers.put(quantifier.getAlias(), quantifier);
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final String alias) {
            return getQuantifier(CorrelationIdentifier.of(alias));
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return Optional.ofNullable(this.quantifiers.get(alias));
        }

        @Nonnull
        public List<Quantifier> getAllQuantifiers() {
            return quantifiers.values().stream().collect(Collectors.toUnmodifiableList());
        }

        @Nonnull
        List<Quantifier> getForEachQuantifiers() {
            return quantifiers.values().stream().filter(q -> q instanceof Quantifier.ForEach).collect(Collectors.toUnmodifiableList());
        }

        public boolean hasQuantifier(@Nonnull final String alias) {
            return hasQuantifier(CorrelationIdentifier.of(alias));
        }

        public boolean hasQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return quantifiers.containsKey(alias);
        }

        public void setPredicate(@Nonnull final QueryPredicate predicate) {
            this.predicate = predicate;
        }

        @Nonnull
        public QueryPredicate getPredicate() {
            if (!hasPredicate()) {
                throw new RecordCoreException("attempt to retrieve non-existing predicate");
            } else {
                Verify.verify(predicate != null);
                return predicate;
            }
        }

        public boolean hasPredicate() {
            return predicate != null;
        }

        public void addProjectionColumn(@Nonnull final Column<? extends Value> column) {
            selectedColumnList.add(column);
            projectedCardinals.add(selectedColumnList.size() - 1);
        }

        public void addOrderByColumn(@Nonnull final Column<? extends Value> column, boolean isDesc) {
            Assert.thatUnchecked(column.getValue() instanceof FieldValue || column.getValue() instanceof VersionValue, "Arbitrary expressions are not allowed in order by clause", ErrorCode.SYNTAX_ERROR);
            if (orderByCardinals.isEmpty()) {
                isReverse = isDesc;
            } else {
                Assert.thatUnchecked(isReverse == isDesc, "Combination of ASC and DESC directions in orderBy clauses is not supported", ErrorCode.UNSUPPORTED_OPERATION);
            }
            if (!getProjectList().contains(column)) {
                selectedColumnList.add(column);
            }
            var orderByCardinalInProjectionList = selectedColumnList.indexOf(column);
            Assert.thatUnchecked(!orderByCardinals.contains(orderByCardinalInProjectionList),
                    String.format("Order by column %s is duplicated in the order by clause", (column.getValue() instanceof VersionValue ? "version" : column.getField().getFieldName())), ErrorCode.COLUMN_ALREADY_EXISTS);
            orderByCardinals.add(selectedColumnList.indexOf(column));
        }

        @Nonnull
        public List<Column<? extends Value>> getProjectList() {
            return selectedColumnList;
        }

        public void setFlag(@Nonnull Flag flag) {
            this.flags.add(flag);
        }

        public void unsetFlag(@Nonnull Flag flag) {
            this.flags.remove(flag);
        }

        public void resetFlags() {
            this.flags.clear();
        }

        public boolean isFlagSet(@Nonnull final Flag flag) {
            return this.flags.contains(flag);
        }

        @Nullable
        public Scope getParent() {
            return parent;
        }

        @Nullable
        public Scope getSibling() {
            return sibling;
        }

        public int getAggregateCounter() {
            return aggCounter;
        }

        public void addAllAggregateReferences(@Nonnull final List<Integer> aggregateReferences) {
            this.aggregateReferences.addAll(aggregateReferences);
        }

        public List<Integer> getAggregateReferences() {
            return aggregateReferences;
        }

        public void addAggregateValue(@Nonnull final AggregateValue newAggregateValue) {
            for (int i = 0; i < aggregateValues.size(); i++) {
                final var aggregateValue = aggregateValues.get(i);
                if (aggregateValue.semanticEquals(newAggregateValue, AliasMap.identitiesFor(aggregateValue.getCorrelatedTo()))) {
                    aggregateReferences.add(i);
                    return;
                }
            }
            aggregateValues.add(newAggregateValue);
            int newAggregateIndex = aggregateValues.size() - 1;
            aggregateReferences.add(newAggregateIndex);
        }

        @Nonnull
        public List<AggregateValue> getAggregateValues() {
            return aggregateValues;
        }

        public void increaseAggCounter() {
            this.aggCounter++;
        }

        public void resetAggCounter() {
            this.aggCounter = 0;
        }

        public void setGroupByQuantifierCorrelation(@Nonnull final CorrelationIdentifier groupByQuantifierCorrelation) {
            this.groupByQuantifierCorrelation = groupByQuantifierCorrelation;
        }

        @Nullable
        public CorrelationIdentifier getGroupByQuantifierCorrelation() {
            return groupByQuantifierCorrelation;
        }

        public void setGroupByType(@Nonnull final Type groupByType) {
            this.groupByType = groupByType;
        }

        @Nullable
        public Type getGroupByType() {
            return groupByType;
        }

        public void setTargetType(@Nonnull final Type targetType) {
            this.targetType = targetType;
        }

        @Nonnull
        public Type getTargetType() {
            if (!hasTargetType()) {
                throw new RecordCoreException("attempt to retrieve non-existing target type");
            } else {
                return Verify.verifyNotNull(targetType);
            }
        }

        public boolean hasTargetType() {
            return targetType != null;
        }

        public void setTargetTypeReorderings(@Nonnull final List<String> targetTypeReorderings) {
            this.targetTypeReorderings = ImmutableList.copyOf(targetTypeReorderings);
        }

        @Nonnull
        public List<String> getTargetTypeReorderings() {
            if (!hasTargetTypeReorderings()) {
                throw new RecordCoreException("attempt to retrieve non-existing target type reorderings");
            } else {
                return Verify.verifyNotNull(targetTypeReorderings);
            }
        }

        public boolean hasTargetTypeReorderings() {
            return targetTypeReorderings != null;
        }

        public static Scope withParent(@Nullable final Scope parent) {
            return withParentAndSibling(parent, null);
        }

        public static Scope withParentAndSibling(@Nullable final Scope parent, @Nullable final Scope sibling) {
            return new Scope(parent, sibling, new LinkedHashMap<>(), new ArrayList<>(), null);
        }
    }
}
