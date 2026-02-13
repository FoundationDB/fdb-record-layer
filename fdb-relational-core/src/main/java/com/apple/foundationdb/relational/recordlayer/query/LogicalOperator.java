/*
 * LogicalOperator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VariadicFunctionValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A logical operator wraps an SQL operation that produces some output. The operator has the following structure:
 * <ul>
 *     <li>optional name, this name usually corresponds to either a SQL alias (e.g. in case of subquery) or a table name</li>
 *     <li>output {@link Expression}s.</li>
 *     <li>internal representation by means of a {@link Quantifier}.</li>
 * </ul>
 * The operator is created when a SQL abstract syntax tree (AST) is traversed [1].
 * For example, the following SQL statement {@code SELECT A, B, C FROM T} will produce a {@link LogicalOperator} whose
 * name is {@code T} and has an output of three {@link Expression}s named {@code A},
 * {@code B}, and {@code C}, the logical operator will wrap an internal quantifier that is a for-each {@link Quantifier}
 * that returns all the rows of the table {@code T}.
 * <br>
 * [1] see {@link com.apple.foundationdb.relational.recordlayer.query.visitors.QueryVisitor} for more information.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class LogicalOperator {

    @Nonnull
    private final Optional<Identifier> name;

    @Nonnull
    private final Expressions output;

    @Nonnull
    private final Quantifier quantifier;

    public LogicalOperator(@Nonnull Optional<Identifier> name,
                           @Nonnull Expressions output,
                           @Nonnull Quantifier quantifier) {
        this.name = name;
        this.output = output;
        this.quantifier = quantifier;
    }

    @Nonnull
    public Optional<Identifier> getName() {
        return name;
    }

    @Nonnull
    public Expressions getOutput() {
        return output;
    }

    @Nonnull
    public Quantifier getQuantifier() {
        return quantifier;
    }

    @Nonnull
    public LogicalOperator withName(@Nonnull Identifier name) {
        if (getName().isPresent() && getName().get().equals(name)) {
            return this;
        }
        if (getName().isEmpty()) {
            return LogicalOperator.newNamedOperator(name, getOutput(), getQuantifier());
        }
        return LogicalOperator.newNamedOperator(name, getOutput().replaceQualifier(existing -> {
            final var prefixSize = getName().get().fullyQualifiedName().size();
            final var existingSize = existing.size();
            if (prefixSize > existingSize) {
                return existing;
            }
            final var existingList = ImmutableList.copyOf(existing);
            if (existingList.subList(0, prefixSize).equals(getName().get().fullyQualifiedName())) {
                return ImmutableList.<String>builder().addAll(name.fullyQualifiedName()).addAll(existingList.subList(prefixSize, existingList.size())).build();
            }
            return existing;
        }), getQuantifier());
    }

    @Nonnull
    public LogicalOperator withAdditionalOutput(@Nonnull Expressions expressions) {
        return LogicalOperator.newOperatorWithPreservedExpressionNames(getName(), output.concat(expressions), getQuantifier());
    }

    @Nonnull
    public LogicalOperator withOutput(@Nonnull Expressions expressions) {
        return LogicalOperator.newOperatorWithPreservedExpressionNames(getName(), expressions, getQuantifier());
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public LogicalOperator withQuantifier(@Nonnull Quantifier quantifier) {
        if (quantifier == getQuantifier()) {
            return this;
        }
        return LogicalOperator.newOperator(getName(), getOutput(), quantifier);
    }

    @Nonnull
    public LogicalOperator withNewSharedReferenceAndAlias(@Nonnull Optional<Identifier> alias) {
        final var quantifier = Quantifier.forEach(getQuantifier().getRangesOver());
        final var result = withOutput(getOutput().rewireQov(quantifier.getFlowedObjectValue())).withQuantifier(quantifier);
        return alias.map(result::withName).orElse(result);
    }

    @Nonnull
    public static LogicalOperator generateAccess(@Nonnull Identifier identifier,
                                                 @Nonnull Optional<Identifier> alias,
                                                 @Nonnull Set<String> requestedIndexes,
                                                 @Nonnull SemanticAnalyzer semanticAnalyzer,
                                                 @Nonnull LogicalPlanFragment currentPlanFragment,
                                                 @Nonnull LogicalOperatorCatalog logicalOperatorCatalog) {
        // look up any localized artifacts, such as common table expressions.
        final var cteMaybe = semanticAnalyzer.findCteMaybe(identifier, currentPlanFragment);
        if (cteMaybe.isPresent()) {
            return cteMaybe.get().withNewSharedReferenceAndAlias(alias);
        } else if (semanticAnalyzer.tableExists(identifier)) {
            return logicalOperatorCatalog.lookupTableAccess(identifier, alias, requestedIndexes, semanticAnalyzer);
        } else if (semanticAnalyzer.viewExists(identifier)) {
            return semanticAnalyzer.resolveView(identifier).withNewSharedReferenceAndAlias(alias);
        } else if (semanticAnalyzer.functionExists(identifier)) {
            return semanticAnalyzer.resolveTableFunction(identifier, Expressions.empty(), false).withNewSharedReferenceAndAlias(alias);
        } else {
            final var correlatedField = semanticAnalyzer.resolveCorrelatedIdentifier(identifier, currentPlanFragment.getLogicalOperatorsIncludingOuter());
            Assert.thatUnchecked(requestedIndexes.isEmpty(), ErrorCode.UNSUPPORTED_QUERY, () -> String.format(Locale.ROOT, "Can not hint indexes with correlated field access %s", identifier));
            return generateCorrelatedFieldAccess(correlatedField, alias);
        }
    }

    @Nonnull
    public static LogicalOperator newNamedOperator(@Nonnull Identifier name,
                                                   @Nonnull Expressions output,
                                                   @Nonnull Quantifier quantifier) {
        return new LogicalOperator(Optional.of(name), output.withQualifier(name), quantifier);
    }

    @Nonnull
    public static LogicalOperator newUnnamedOperator(@Nonnull Expressions output,
                                                     @Nonnull Quantifier quantifier) {
        return new LogicalOperator(Optional.empty(), output.clearQualifier(), quantifier);
    }

    @Nonnull
    public static LogicalOperator newOperator(@Nonnull Optional<Identifier> name,
                                              @Nonnull Expressions output,
                                              @Nonnull Quantifier quantifier) {
        return name.map(identifier -> newNamedOperator(identifier, output, quantifier))
                .orElseGet(() -> newUnnamedOperator(output, quantifier));
    }

    @Nonnull
    public static LogicalOperator newOperatorWithPreservedExpressionNames(@Nonnull Expressions output,
                                                                          @Nonnull Quantifier quantifier) {
        return newOperatorWithPreservedExpressionNames(Optional.empty(), output, quantifier);
    }

    @Nonnull
    public static LogicalOperator newOperatorWithPreservedExpressionNames(@Nonnull Optional<Identifier> name,
                                                                          @Nonnull Expressions output,
                                                                          @Nonnull Quantifier quantifier) {
        return new LogicalOperator(name, output, quantifier);
    }

    @Nonnull
    public static LogicalOperator generateTableAccess(@Nonnull Identifier tableId,
                                                      @Nonnull Set<AccessHint> indexAccessHints,
                                                      @Nonnull SemanticAnalyzer semanticAnalyzer) {
        final Set<String> tableNames = semanticAnalyzer.getAllTableStorageNames();
        semanticAnalyzer.validateIndexes(tableId, indexAccessHints);
        final var scanExpression = Quantifier.forEach(Reference.initialOf(
                new FullUnorderedScanExpression(tableNames,
                        new Type.AnyRecord(false),
                        new AccessHints(indexAccessHints.toArray(new AccessHint[0])))));
        final var table = semanticAnalyzer.getTable(tableId);
        Type.Record type = Assert.castUnchecked(table, RecordLayerTable.class).getType();
        if (semanticAnalyzer.getMetadataCatalog().isStoreRowVersions()) {
            // Ideally, the RecordLayerTable would have the full type with all the pseudo-fields,
            // but we need star expansion to skip over these fields. That would be made easier
            // if we fully supported invisible columns (see: https://github.com/FoundationDB/fdb-record-layer/pull/3787)
            type = type.addPseudoFields();
        }
        final String storageName = type.getStorageName();
        Assert.thatUnchecked(storageName != null, "storage name for table access must not be null");
        final var typeFilterExpression = new LogicalTypeFilterExpression(ImmutableSet.of(storageName), scanExpression, type);
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(typeFilterExpression));
        final ImmutableList.Builder<Expression> attributesBuilder = ImmutableList.builder();
        int colCount = 0;
        for (final var column : table.getColumns()) {
            final var attributeName = Identifier.of(column.getName());
            final var attributeType = column.getDataType();
            final var fieldType = type.getField(colCount);
            final var attributeExpression = FieldValue.ofFields(resultingQuantifier.getFlowedObjectValue(),
                    FieldValue.FieldPath.ofSingle(FieldValue.ResolvedAccessor.of(fieldType, colCount)));
            attributesBuilder.add(new Expression(Optional.of(attributeName), attributeType, attributeExpression));
            colCount++;
        }
        if (semanticAnalyzer.getMetadataCatalog().isStoreRowVersions()
                && table.getColumns().stream().noneMatch(column -> column.getName().equals(PseudoField.ROW_VERSION.getFieldName()))) {
            final var pseudoFieldValue = FieldValue.ofFieldName(resultingQuantifier.getFlowedObjectValue(), PseudoField.ROW_VERSION.getFieldName());
            final Expression pseudoExpression = new Expression(Optional.of(Identifier.of(PseudoField.ROW_VERSION.getFieldName())), DataType.VersionType.nullable(), pseudoFieldValue);
            attributesBuilder.add(pseudoExpression.asEphemeral());
        }
        final var attributes = Expressions.of(attributesBuilder.build());
        return LogicalOperator.newNamedOperator(tableId, attributes, resultingQuantifier);
    }

    @Nonnull
    private static LogicalOperator generateCorrelatedFieldAccess(@Nonnull Expression expression,
                                                                 @Nonnull Optional<Identifier> alias) {
        Assert.thatUnchecked(expression.getDataType().getCode() == DataType.Code.ARRAY,
                ErrorCode.INVALID_COLUMN_REFERENCE,
                () -> String.format(Locale.ROOT, "join correlation can occur only on column of repeated type, not %s type", expression.getDataType()));
        final var explode = new ExplodeExpression(expression.getUnderlying());
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(explode));

        Expressions outputAttributes;
        if (resultingQuantifier.getFlowedObjectType().isPrimitive()) {
            final ImmutableList.Builder<Expression> attributesBuilder = ImmutableList.builder();
            attributesBuilder.add(new Expression(alias, DataTypeUtils.toRelationalType(explode.getResultValue().getResultType()), resultingQuantifier.getFlowedObjectValue()));
            outputAttributes = Expressions.of(attributesBuilder.build());
        } else {
            outputAttributes = Expressions.of(convertToExpressions(resultingQuantifier));
        }
        return LogicalOperator.newOperator(alias, outputAttributes, resultingQuantifier);
    }

    @Nonnull
    public static Expressions convertToExpressions(@Nonnull Quantifier quantifier) {
        final ImmutableList.Builder<Expression> attributesBuilder = ImmutableList.builder();
        int colCount = 0;
        final var columns = quantifier.getFlowedColumns();
        for (final var column : columns) {
            final var field = column.getField();
            final var value = column.getValue();
            final var attributeName = field.getFieldNameOptional().map(Identifier::of);
            final var attributeType = DataTypeUtils.toRelationalType(value.getResultType());
            final var attributeExpression = FieldValue.ofOrdinalNumber(quantifier.getFlowedObjectValue(), colCount);
            attributesBuilder.add(new Expression(attributeName, attributeType, attributeExpression));
            colCount++;
        }
        return Expressions.of(attributesBuilder.build());
    }

    @Nonnull
    public static LogicalOperator generateSelect(@Nonnull Expressions output,
                                                 @Nonnull LogicalOperators logicalOperators,
                                                 @Nonnull Optional<Expression> predicate,
                                                 @Nonnull List<OrderByExpression> orderBys,
                                                 @Nonnull Optional<Identifier> alias,
                                                 @Nonnull Set<CorrelationIdentifier> outerCorrelations,
                                                 boolean isTopLevel,
                                                 boolean isForDdl) {
        if (orderBys.isEmpty()) {
            if (isTopLevel) {
                return generateSort(generateSimpleSelect(output, logicalOperators, predicate, Optional.empty(), outerCorrelations, isForDdl), orderBys, outerCorrelations, alias);
            }
            return generateSimpleSelect(output, logicalOperators, predicate, alias, outerCorrelations, isForDdl);
        }
        final var orderByExpressions = Expressions.of(orderBys.stream().map(OrderByExpression::getExpression).collect(ImmutableList.toImmutableList()));
        final var remainingOrderByExpressions = orderByExpressions.difference(output, outerCorrelations);
        if (remainingOrderByExpressions.isEmpty()) {
            return generateSort(generateSimpleSelect(output, logicalOperators, predicate, Optional.empty(), outerCorrelations, isForDdl), orderBys, outerCorrelations, alias);
        } else {
            final var selectWithExtraOrderByExpressions = output.concat(remainingOrderByExpressions);
            final var selectWithExtraOrderBy = generateSimpleSelect(selectWithExtraOrderByExpressions, logicalOperators, predicate, Optional.empty(), outerCorrelations, isForDdl);
            final var sortOperator = generateSort(selectWithExtraOrderBy, orderBys, outerCorrelations, Optional.empty());
            final var pulledOutput = output.expanded().rewireQov(selectWithExtraOrderBy.getQuantifier().getFlowedObjectValue())
                    .rewireQov(sortOperator.getQuantifier().getFlowedObjectValue()).clearQualifier();
            return generateSimpleSelect(pulledOutput, LogicalOperators.ofSingle(sortOperator), Optional.empty(), alias, outerCorrelations, isForDdl);
        }
    }

    @Nonnull
    public static LogicalOperator generateSelectWhere(@Nonnull LogicalOperators logicalOperators,
                                                      @Nonnull Set<CorrelationIdentifier> outerCorrelations,
                                                      @Nonnull Optional<Expression> where,
                                                      boolean isForDdl) {
        final var quantifiers = logicalOperators.getQuantifiers();
        final var quantifiedObjectValues = quantifiers.stream().map(QuantifiedObjectValue::of).collect(ImmutableList.toImmutableList());
        final var selectBuilder = GraphExpansion.builder().addAllQuantifiers(quantifiers).addAllResultValues(quantifiedObjectValues);
        where.ifPresent(predicate -> {
            final var localAliases = quantifiers.stream().map(Quantifier::getAlias).collect(ImmutableSet.toImmutableSet());
            selectBuilder.addPredicate(Expression.Utils.toUnderlyingPredicate(predicate, localAliases, isForDdl));
        });
        final var selectExpression = selectBuilder.build().buildSelect();
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(selectExpression));
        final var expressions = logicalOperators.getExpressions();
        final var output = expressions.pullUp(selectExpression.getResultValue(), resultingQuantifier.getAlias(), outerCorrelations);
        return LogicalOperator.newOperatorWithPreservedExpressionNames(output, resultingQuantifier);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static LogicalOperator generateGroupBy(@Nonnull LogicalOperators logicalOperators,
                                                  @Nonnull Expressions groupByExpressions,
                                                  @Nonnull Expressions outputExpressions,
                                                  @Nonnull Optional<Expression> havingPredicate,
                                                  @Nonnull Set<CorrelationIdentifier> outerCorrelations,
                                                  @Nonnull Literals literals) {
        final var aliasMap = AliasMap.identitiesFor(logicalOperators.getCorrelations());
        final var aggregates = Expressions.of(havingPredicate.map(outputExpressions::concat).orElse(outputExpressions)
                .collectAggregateValues().stream().map(Expression::fromUnderlying).collect(ImmutableSet.toImmutableSet()));
        SemanticAnalyzer.validateGroupByAggregates(aggregates);
        final var validSubExpressions = groupByExpressions.dereferenced(literals).concat(aggregates.dereferenced(literals));

        for (final var expression : outputExpressions.expanded().concat(havingPredicate.map(Expressions::ofSingle).orElseGet(Expressions::empty))) {
            Assert.thatUnchecked(SemanticAnalyzer.isComposableFrom(expression.dereferenced(literals).getSingleItem(), validSubExpressions, aliasMap, outerCorrelations),
                    ErrorCode.GROUPING_ERROR,
                    () -> String.format(Locale.ROOT, "Invalid reference to non-grouping expression %s", expression));
        }

        final var aggregateValue = RecordConstructorValue.ofUnnamed((List<Value>) Assert.castUnchecked(aggregates.underlying(), List.class));
        final var groupingValue = RecordConstructorValue.ofUnnamed((List<Value>) Assert.castUnchecked(groupByExpressions.underlying(), List.class));

        final var groupByExpression = new GroupByExpression(groupingValue.getColumns().isEmpty() ? null : groupingValue, aggregateValue,
                GroupByExpression::nestedResults, Iterables.getOnlyElement(logicalOperators).quantifier);

        final var groupByReference = Reference.initialOf(groupByExpression);
        final var resultingQuantifier = groupByExpression.getGroupingValue() == null
                                        ? Quantifier.forEachWithNullOnEmpty(groupByReference)
                                        : Quantifier.forEach(groupByReference);

        // this must be aligned with _how_ the GroupByExpression composes its result set.
        final var output = groupByExpressions.concat(aggregates).pullUp(groupByExpression.getResultValue(), resultingQuantifier.getAlias(),
                outerCorrelations).clearQualifier();

        return LogicalOperator.newUnnamedOperator(output, resultingQuantifier);
    }

    @Nonnull
    public static LogicalOperator generateSimpleSelect(@Nonnull Expressions output,
                                                       @Nonnull LogicalOperators logicalOperators,
                                                       @Nonnull Optional<Expression> where,
                                                       @Nonnull Optional<Identifier> alias,
                                                       @Nonnull Set<CorrelationIdentifier> outerCorrelations,
                                                       boolean isForDdl) {
        final var quantifiers = logicalOperators.getQuantifiers();
        final var selectBuilder = GraphExpansion.builder().addAllQuantifiers(quantifiers);
        where.ifPresent(predicate -> {
            final var localAliases = quantifiers.stream().map(Quantifier::getAlias).collect(ImmutableSet.toImmutableSet());
            selectBuilder.addPredicate(Expression.Utils.toUnderlyingPredicate(predicate, localAliases, isForDdl));
        });
        final var expandedOutput = output.expanded();
        SelectExpression selectExpression;

        if (canAvoidProjectingIndividualFields(output, logicalOperators)) {
            final var passedThroughResultValue = Iterables.getOnlyElement(output).getUnderlying();
            selectExpression = selectBuilder.build().buildSelectWithResultValue(passedThroughResultValue);
        } else {
            expandedOutput.underlyingAsColumns().forEach(selectBuilder::addResultColumn);
            selectExpression = selectBuilder.build().buildSelect();
        }

        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(selectExpression));
        var resultingExpressions = expandedOutput.rewireQov(resultingQuantifier.getFlowedObjectValue());
        resultingExpressions = alias.map(resultingExpressions::withQualifier).orElseGet(resultingExpressions::clearQualifier);
        return LogicalOperator.newOperator(alias, resultingExpressions, resultingQuantifier);
    }

    /**
     * Determine whether it is possible to skip projection of individual columns of the underlying quantifier. This is
     * to avoid unnecessary "breaking" a record constructor value unnecessarily when the user issues a query as simple
     * as {@code SELECT * FROM T}.
     * <br/>
     * It can be thought of as a premature optimization considering and should be done by the optimizer during an initial
     * plan canonicalization phase.
     *
     * @param output the {@link LogicalOperator}'s output.
     * @param logicalOperators The underlying logical operators.
     * @return {@code true} if projecting individual columns of the underlying quantifier can be avoided, otherwise
     * {@code false}.
     */
    private static boolean canAvoidProjectingIndividualFields(@Nonnull Expressions output,
                                                              @Nonnull LogicalOperators logicalOperators) {
        // No joins
        if (Iterables.size(logicalOperators.forEachOnly()) != 1 || Iterables.size(output) != 1) {
            return false;
        }
        // Must be a star expression
        final Expression outputExpression = Iterables.getOnlyElement(output);
        if (!(outputExpression instanceof Star)) {
            return false;
        }
        // Get the set of fields coming from the underlying value
        final Type underlyingType = outputExpression.getUnderlying().getResultType();
        if (!(underlyingType instanceof Type.Record)) {
            return false;
        }
        final List<Type.Record.Field> underlyingFields = ((Type.Record)underlyingType).getFields();

        // Compare the set of fields coming from the expansion. We can only skip the expansion
        // if the result fields match exactly. That is, we have the same set of fields coming
        // from the expansion as are in the underlying result value, and they all have the same
        // names. We have to make this test for two reasons:
        //   1. There may be pseudo-fields coming from the underlying expression that are then
        //      missing from the expansion. That will show up here as extra columns in the
        //      underlying type that are missing from the expansion iterator.
        //   2. CTEs can reference aliased columns of a named query. We need to validate
        //      that these columns are not aliased differently in the underlying query
        //      fragment, so we need to pairwise match them to the result type below
        final Expressions expanded = output.expanded();
        if (expanded.size() != underlyingFields.size()) {
            return false;
        }
        final Iterator<Expression> expansionIterator = expanded.iterator();
        final Iterator<Type.Record.Field> fieldIterator = underlyingFields.iterator();
        while (expansionIterator.hasNext() && fieldIterator.hasNext()) {
            final Expression expandedExpression = expansionIterator.next();
            final Type.Record.Field underlyingField = fieldIterator.next();
            if (expandedExpression.getName().isEmpty()) {
                continue;
            }
            if (!(expandedExpression.getUnderlying() instanceof FieldValue)
                    || !expandedExpression.getName().map(Identifier::getName).equals(underlyingField.getFieldNameOptional())) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    public static LogicalOperator generateSort(@Nonnull LogicalOperator logicalOperator,
                                               @Nonnull List<OrderByExpression> orderBys,
                                               @Nonnull Set<CorrelationIdentifier> outerCorrelations,
                                               @Nonnull Optional<Identifier> alias) {
        final LogicalSortExpression sortExpression;
        if (orderBys.isEmpty()) {
            sortExpression = LogicalSortExpression.unsorted(logicalOperator.quantifier);
        } else {
            final var orderingParts = OrderByExpression.toOrderingParts(
                    OrderByExpression.pullUp(orderBys.stream(),
                            logicalOperator.quantifier.getRangesOver().get().getResultValue(),
                            logicalOperator.quantifier.getAlias(),
                            outerCorrelations,
                            logicalOperator.name),
                    logicalOperator.quantifier.getAlias(), Quantifier.current())
                    .collect(ImmutableList.toImmutableList());
            sortExpression = new LogicalSortExpression(
                    RequestedOrdering.ofPrimitiveParts(orderingParts, RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS, false),
                    logicalOperator.quantifier);
        }

        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(sortExpression));
        // the resulting sort expression has exactly the same output as the underlying expression.
        var resultingExpressions = Expressions.of(logicalOperator.output).rewireQov(resultingQuantifier.getFlowedObjectValue());
        resultingExpressions = alias.map(resultingExpressions::withQualifier).orElseGet(resultingExpressions::clearQualifier);
        return LogicalOperator.newOperator(alias, resultingExpressions, resultingQuantifier);
    }

    @Nonnull
    public static LogicalOperator generateInsert(@Nonnull LogicalOperator insertSource, @Nonnull Table target) {
        final Type.Record targetType = Assert.castUnchecked(target, RecordLayerTable.class).getType();
        final var insertExpression = new InsertExpression(Assert.castUnchecked(insertSource.getQuantifier(),
                        Quantifier.ForEach.class),
                Assert.notNullUnchecked(targetType.getStorageName(), "target type for insert must have set storage name"),
                targetType);
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(insertExpression));
        final var output = Expressions.fromQuantifier(resultingQuantifier);
        final var insertOperator = LogicalOperator.newUnnamedOperator(output, resultingQuantifier);
        return generateSort(insertOperator, List.of(), Set.of(), Optional.empty());
    }

    @Nonnull
    public static CorrelationIdentifier getInnermostAlias(@Nonnull Iterable<LogicalOperator> logicalOperators) {
        final Collection<CorrelationIdentifier> aliases = Streams.stream(logicalOperators)
                .map(LogicalOperator::getQuantifier)
                .filter(qun -> qun instanceof Quantifier.ForEach)
                .map(Quantifier::getAlias)
                .collect(Collectors.toList()); // not sure if this is correct
        return aliases.stream().findFirst().orElseThrow();
    }

    @Nonnull
    public static LogicalOperator generateUnionAll(@Nonnull LogicalOperators unionLegs,
                                                   @Nonnull Set<CorrelationIdentifier> outerCorrelations) {
        Assert.thatUnchecked(!unionLegs.isEmpty());
        if (unionLegs.size() == 1) {
            return unionLegs.first();
        }
        final var quantifiers = unionLegs.getQuantifiers();
        final var maybeType = SemanticAnalyzer.validateUnionTypes(LogicalOperators.of(unionLegs));
        if (maybeType.isEmpty()) {
            // proceed to create a vanilla union all.
            final var union = Quantifier.forEach(Reference.initialOf(new LogicalUnionExpression(quantifiers)));
            final var output = unionLegs.first().getOutput().rewireQov(union.getFlowedObjectValue());
            return LogicalOperator.newUnnamedOperator(output, union);
        }

        // create union all with promotions.
        // navigate to the actual fields and perform the promotion; a better way of doing this would be to
        // promote (if necessary) the entire record, once we have this resolved:
        // TODO (Make type promotion logic able to work with nested data types)
        final var type = maybeType.get();
        final ImmutableList.Builder<LogicalOperator> promotedUnionLegsBuilder = ImmutableList.builder();
        for (final var unionLeg : unionLegs) {
            final var unionLegType = unionLeg.getQuantifier().getFlowedObjectType();
            if (unionLegType.equals(type)) {
                promotedUnionLegsBuilder.add(unionLeg);
                continue;
            }
            final var expressions = unionLeg.getOutput();
            Assert.thatUnchecked(expressions.size() == type.getFields().size());
            final ImmutableList.Builder<Expression> promotedExpressions = ImmutableList.builder();
            for (int i = 0; i < expressions.size(); i++) {
                final var currentExpression = expressions.asList().get(i);
                final var newValue = PromoteValue.inject(currentExpression.getUnderlying(), type.getField(i).getFieldType());
                promotedExpressions.add(currentExpression.withUnderlying(newValue));
            }
            final var promotedUnionLeg = LogicalOperator.generateSimpleSelect(Expressions.of(promotedExpressions.build()),
                    LogicalOperators.ofSingle(unionLeg), Optional.empty(), Optional.empty(), outerCorrelations, false);
            promotedUnionLegsBuilder.add(promotedUnionLeg);
        }
        final var promotedUnionLegs = LogicalOperators.of(promotedUnionLegsBuilder.build());
        final var union = Quantifier.forEach(Reference.initialOf(new LogicalUnionExpression(promotedUnionLegs.getQuantifiers())));
        final var output = promotedUnionLegs.first().getOutput().rewireQov(union.getFlowedObjectValue());
        return LogicalOperator.newUnnamedOperator(output, union);
    }

    @Nonnull
    public static Expressions adjustCountOnEmpty(@Nonnull final Expressions expressions) {
        return Expressions.of(expressions.expanded().stream().map(expression -> {
            final var underlyingValue = expression.getUnderlying();
            final Set<Value> visited = Sets.newIdentityHashSet();
            return expression.withUnderlying(Objects.requireNonNull(underlyingValue.replace(subValue -> {
                if (!visited.add(subValue)) {
                    return subValue;
                }
                // as per SQL standard section 4.16.4 (Aggregate functions):
                // "If no row qualifies, then the result of COUNT is 0 (zero), and the result of any other aggregate function is the null value.
                if (subValue instanceof CountValue) {
                    final var zero = LiteralValue.ofScalar(0L);
                    return (Value) new VariadicFunctionValue.CoalesceFn().encapsulate(ImmutableList.of(subValue, zero));
                }
                return subValue;
            })));
        }).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    public static LogicalOperator newTemporaryTableScan(@Nonnull final Identifier operatorId,
                                                        @Nonnull final Identifier tempTableId,
                                                        @Nonnull final Type type) {
        final var tempTableAlias = CorrelationIdentifier.of(tempTableId.getName());
        final var tempTableScan = TempTableScanExpression.ofCorrelated(tempTableAlias, type);
        final var quantifier = Quantifier.forEach(Reference.initialOf(tempTableScan));
        final var expressions = Expressions.fromQuantifier(quantifier);
        return LogicalOperator.newNamedOperator(operatorId, expressions, quantifier);
    }

    @Nonnull
    public static LogicalOperator newTemporaryTableInsert(@Nonnull final LogicalOperator input,
                                                          @Nonnull final Identifier identifier,
                                                          @Nonnull final Type type) {
        final var tempTableAlias = CorrelationIdentifier.of(identifier.getName());
        final var tempTableInsert = TempTableInsertExpression.ofCorrelated(input.getQuantifier().narrow(Quantifier.ForEach.class),
                tempTableAlias, type);
        final var quantifier = Quantifier.forEach(Reference.initialOf(tempTableInsert));
        final var expressions = Expressions.fromQuantifier(quantifier);
        return LogicalOperator.newUnnamedOperator(expressions, quantifier);
    }
}
