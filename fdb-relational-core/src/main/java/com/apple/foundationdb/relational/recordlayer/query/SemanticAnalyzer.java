/*
 * SemanticAnalyzer.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInTableFunction;
import com.apple.foundationdb.record.query.plan.cascades.CatalogedFunction;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerView;
import com.apple.foundationdb.relational.recordlayer.query.functions.SqlFunctionCatalog;
import com.apple.foundationdb.relational.recordlayer.query.functions.WithPlanGenerationSideEffects;
import com.apple.foundationdb.relational.recordlayer.query.visitors.QueryVisitor;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for performing a number of tasks revolving around semantic checks and resolution. For example,
 * it assists in looking up an {@link Identifier} within a chain of {@link LogicalPlanFragment}(s), expanding a {@link Star}
 * expression into its individual fields, resolving an alias, and so on.
 * <br/>
 * In addition to that, this class performs metadata resolution tasks, such as resolving tables, validating indexes, and
 * resolving built-in and user-defined functions, which arguably, should be handled in a separate catalog component.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class SemanticAnalyzer {

    private static final int BITMAP_DEFAULT_ENTRY_SIZE = 10_000;

    private static final Set<String> BITMAP_SCALAR_FUNCTIONS = ImmutableSet.of("bitmap_bucket_offset", "bitmap_bit_position");

    @Nonnull
    private final SchemaTemplate metadataCatalog;

    @Nonnull
    private final SqlFunctionCatalog functionCatalog;

    @Nonnull
    private final MutablePlanGenerationContext mutablePlanGenerationContext;

    private final boolean isCaseSensitive;

    public SemanticAnalyzer(@Nonnull SchemaTemplate metadataCatalog,
                            @Nonnull SqlFunctionCatalog functionCatalog,
                            @Nonnull MutablePlanGenerationContext mutablePlanGenerationContext,
                            boolean isCaseSensitive) {
        this.metadataCatalog = metadataCatalog;
        this.functionCatalog = functionCatalog;
        this.mutablePlanGenerationContext = mutablePlanGenerationContext;
        this.isCaseSensitive = isCaseSensitive;
    }

    /**
     * If a string is single- or double-quoted, removes the quotation, otherwise, upper-case it.
     *
     * @param string The input string
     * @param caseSensitive if {@code true}, the input string is taken as-is, upper-cased otherwise
     * @return the normalized string
     */
    @Nullable
    public static String normalizeString(@Nullable final String string, boolean caseSensitive) {
        if (string == null) {
            return null;
        }
        if (isQuoted(string, "'") || isQuoted(string, "\"")) {
            return string.substring(1, string.length() - 1);
        } else if (caseSensitive) {
            return string;
        } else {
            return string.toUpperCase(Locale.ROOT);
        }
    }

    /**
     * Checks whether a string is properly quoted (same quote markers at the beginning and at the end).
     * @param str The input string.
     * @param quotationMark The quotation mark to look for
     * @return <code>true</code> is the string is quoted, otherwise <code>false</code>.
     */
    private static boolean isQuoted(@Nonnull final String str, @Nonnull final String quotationMark) {
        return str.startsWith(quotationMark) && str.endsWith(quotationMark);
    }

    /**
     * Looks up a common table expression (CTE) in a chain of {@link LogicalPlanFragment}.
     * @param identifier The name of the CTE.
     * @param logicalPlanFragment The plan fragments chain.
     * @return Optional with {@link LogicalOperator} whose name matches the given identifier, if found.
     */
    @Nonnull
    public Optional<LogicalOperator> findCteMaybe(@Nonnull final Identifier identifier,
                                                  @Nonnull final LogicalPlanFragment logicalPlanFragment) {
        var currentFragment = Optional.of(logicalPlanFragment);
        while (currentFragment.isPresent()) {
            final var logicalOperators = currentFragment.get().getLogicalOperators();
            final var matches = logicalOperators.stream().filter(logicalOperator -> logicalOperator.getName().isPresent() &&
                    logicalOperator.getName().get().equals(identifier)).collect(ImmutableList.toImmutableList());
            Assert.thatUnchecked(matches.size() <= 1, ErrorCode.DUPLICATE_ALIAS,
                    () -> String.format(Locale.ROOT, "found '%s' more than once", identifier.getName()));
            if (!matches.isEmpty()) {
                return Optional.of(matches.get(0));
            }
            currentFragment = currentFragment.get().getParentMaybe();
        }
        return Optional.empty();
    }

    public boolean tableExists(@Nonnull final Identifier tableIdentifier) {
        if (tableIdentifier.getQualifier().size() > 1) {
            return false;
        }

        if (tableIdentifier.isQualified()) {
            final var qualifier = tableIdentifier.getQualifier().get(0);
            if (!metadataCatalog.getName().equals(qualifier)) {
                return false;
            }
        }

        final var tableName = tableIdentifier.getName();
        try {
            return metadataCatalog.findTableByName(tableName).isPresent();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    public boolean viewExists(@Nonnull final Identifier viewIdentifier) {
        if (viewIdentifier.getQualifier().size() > 1) {
            return false;
        }

        if (viewIdentifier.isQualified()) {
            final var qualifier = viewIdentifier.getQualifier().get(0);
            if (!metadataCatalog.getName().equals(qualifier)) {
                return false;
            }
        }

        final var viewName = viewIdentifier.getName();
        try {
            return metadataCatalog.findViewByName(viewName).isPresent();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    public boolean functionExists(@Nonnull final Identifier functionIdentifier) {
        return functionCatalog.containsFunction(functionIdentifier.getName());
    }

    @Nonnull
    public Table getTable(@Nonnull Identifier tableIdentifier) {
        Assert.thatUnchecked(tableIdentifier.getQualifier().size() <= 1, ErrorCode.INTERNAL_ERROR, () -> String.format(Locale.ROOT, "Unknown table %s", tableIdentifier));
        if (tableIdentifier.isQualified()) {
            final var qualifier = tableIdentifier.getQualifier().get(0);
            Assert.thatUnchecked(metadataCatalog.getName().equals(qualifier), ErrorCode.UNDEFINED_DATABASE, () -> String.format(Locale.ROOT, "Unknown schema template %s", qualifier));
        }
        final var tableName = tableIdentifier.getName();
        try {
            final var tableMaybe = metadataCatalog.findTableByName(tableName);
            Assert.thatUnchecked(tableMaybe.isPresent(), ErrorCode.UNDEFINED_TABLE, () -> String.format(Locale.ROOT, "Unknown table %s", tableName));
            return tableMaybe.get();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    public void validateIndexes(@Nonnull Identifier tableIdentifier, @Nonnull Set<AccessHint> requestedIndexes) {
        final var table = getTable(tableIdentifier);
        validateIndexes(table, requestedIndexes);
    }

    public void validateIndexes(@Nonnull Table table, @Nonnull Set<AccessHint> requestedIndexes) {
        if (requestedIndexes.isEmpty()) {
            return;
        }
        final var nonIndexAccessHints = requestedIndexes.stream().filter(accessHint -> !(accessHint instanceof IndexAccessHint))
                .map(Object::getClass).map(Class::toString).collect(ImmutableSet.toImmutableSet());
        Assert.thatUnchecked(nonIndexAccessHints.isEmpty(), ErrorCode.UNDEFINED_INDEX, () -> String.format(Locale.ROOT, "Unknown index hint(s) %s", String.join(",", nonIndexAccessHints)));
        final var tableIndexes = table.getIndexes().stream().map(Metadata::getName).collect(ImmutableSet.toImmutableSet());
        final var unrecognizedIndexes = Sets.difference(requestedIndexes.stream().map(IndexAccessHint.class::cast).map(IndexAccessHint::getIndexName).collect(ImmutableSet.toImmutableSet()), tableIndexes);
        Assert.thatUnchecked(unrecognizedIndexes.isEmpty(), ErrorCode.UNDEFINED_INDEX, () -> String.format(Locale.ROOT, "Unknown index(es) %s", String.join(",", unrecognizedIndexes)));
    }

    public void validateOrderByColumns(@Nonnull Iterable<OrderByExpression> orderBys) {
        final var duplicates = StreamSupport.stream(orderBys.spliterator(), false)
                .map(OrderByExpression::getExpression)
                .flatMap(expr -> expr.getName().stream())
                .collect(Collectors.groupingBy(Functions.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(p -> p.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        Assert.thatUnchecked(duplicates.isEmpty(), ErrorCode.COLUMN_ALREADY_EXISTS,
                () -> String.format(Locale.ROOT, "Order by column %s is duplicated in the order by clause",
                        duplicates.stream().map(Identifier::toString).collect(Collectors.joining(","))));
    }

    @Nonnull
    public Set<String> getAllTableStorageNames() {
        try {
            return metadataCatalog.getTables().stream()
                    .map(table -> Assert.castUnchecked(table, RecordLayerTable.class))
                    .map(table -> Assert.notNullUnchecked(table.getType().getStorageName()))
                    .collect(ImmutableSet.toImmutableSet());
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Nonnull
    public Expression resolveCorrelatedIdentifier(@Nonnull Identifier identifier,
                                                  @Nonnull LogicalOperators operators) {
        Assert.thatUnchecked(identifier.isQualified(), ErrorCode.UNDEFINED_TABLE, () -> String.format(Locale.ROOT, "Unknown table %s", identifier));
        return resolveIdentifier(identifier, operators);
    }

    @Nonnull
    public Star expandStar(@Nonnull Optional<Identifier> optionalQualifier,
                           @Nonnull LogicalOperators operators) {
        final var forEachOperators = operators.forEachOnly();
        // Case 1: no qualifier, e.g. SELECT * FROM T, R;
        if (optionalQualifier.isEmpty()) {
            final var expansion = forEachOperators.getExpressions().nonEphemeral().nonHidden();
            return Star.overQuantifiers(Optional.empty(), Streams.stream(forEachOperators).map(LogicalOperator::getQuantifier)
                    .map(Quantifier::getFlowedObjectValue).collect(ImmutableList.toImmutableList()), "unknown", expansion);
        }
        // Case 2: qualifying a table, e.g. SELECT T.* FROM T, R;
        final var qualifier = optionalQualifier.get();
        final var logicalTableMaybe = Streams.stream(forEachOperators)
                .filter(table -> table.getName().isPresent() && table.getName().get().equals(qualifier))
                .findFirst();
        if (logicalTableMaybe.isPresent()) {
            return Star.overQuantifier(optionalQualifier, logicalTableMaybe.get().getQuantifier().getFlowedObjectValue(),
                    qualifier.getName(), logicalTableMaybe.get().getOutput().nonEphemeral().nonHidden());
        }
        // Case 2.1: represents a rare case where a logical operator contains a mix of columns that are qualified
        // differently.
        // This mostly happens when the logical operator encompasses an internal modeling strategy
        // rather than adhering to what the user _can_ semantically describe in SQL.
        final var individualReferencedColumns = Expressions.of(forEachOperators.getExpressions().stream()
                .filter(expr -> expr.getName().isPresent() && expr.getName().get().isQualified())
                .filter(expr -> expr.getName().get().qualifiedWith(optionalQualifier.get()))
                .collect(ImmutableList.toImmutableList()));
        if (!individualReferencedColumns.isEmpty()) {
            return Star.overIndividualExpressions(optionalQualifier, "unknown", individualReferencedColumns);
        }
        // Case 3: qualifying a column inside a table, e.g. SELECT T.A.* FROM T, R;
        // TODO this is currently not supported as per parsing rules TODO (Expand nested struct fields)
        final var expression = resolveIdentifier(qualifier, forEachOperators);
        Assert.thatUnchecked(expression.getDataType().getCode() == DataType.Code.STRUCT, ErrorCode.INVALID_COLUMN_REFERENCE,
                () -> String.format(Locale.ROOT, "attempt to expand non-struct column %s", qualifier));
        final var expressions = expandStructExpression(expression).nonEphemeral().nonHidden();
        return Star.overQuantifier(optionalQualifier, expression.getUnderlying(), qualifier.getName(), expressions);
    }

    @Nonnull
    public Expression resolveIdentifier(@Nonnull Identifier identifier,
                                        @Nonnull LogicalPlanFragment planFragment) {
        // search throw all visible plan fragments:
        // - in each plan fragment, search operators left to right.
        // - if identifier is not resolve, go to parent plan fragment.
        LogicalPlanFragment currentPlanFragment = planFragment;
        var resolvedMaybe = resolveIdentifierMaybe(identifier, currentPlanFragment.getLogicalOperators());
        if (resolvedMaybe.isPresent()) {
            return resolvedMaybe.get();
        }
        while (currentPlanFragment.hasParent()) {
            currentPlanFragment = currentPlanFragment.getParent();
            resolvedMaybe = resolveIdentifierMaybe(identifier, currentPlanFragment.getLogicalOperators());
            if (resolvedMaybe.isPresent()) {
                return resolvedMaybe.get();
            }
        }
        Assert.failUnchecked(ErrorCode.UNDEFINED_COLUMN, String.format(Locale.ROOT, "Attempting to query non existing column %s", identifier));
        return null; // unreachable.
    }

    @Nonnull
    public Expression resolveIdentifier(@Nonnull Identifier identifier,
                                        @Nonnull LogicalOperators operators) {
        var attributes = lookup(identifier, operators, true);
        Assert.thatUnchecked(attributes.size() <= 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        if (attributes.isEmpty()) {
            attributes = lookup(identifier, operators, false);
        }
        Assert.thatUnchecked(!attributes.isEmpty(), ErrorCode.UNDEFINED_COLUMN, () -> String.format(Locale.ROOT, "Unknown reference %s", identifier));
        Assert.thatUnchecked(attributes.size() == 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        return attributes.get(0);
    }

    public Expression resolveIdentifier(@Nonnull Identifier identifier,
                                        @Nonnull LogicalOperator operator) {
        var attributes = lookupOneOperaror(identifier, operator, true);
        Assert.thatUnchecked(attributes.size() <= 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        if (attributes.isEmpty()) {
            attributes = lookupOneOperaror(identifier, operator, false);
        }
        Assert.thatUnchecked(!attributes.isEmpty(), ErrorCode.UNDEFINED_COLUMN, () -> String.format(Locale.ROOT, "Unknown reference %s", identifier));
        Assert.thatUnchecked(attributes.size() == 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        return attributes.get(0);
    }

    @Nonnull
    private Optional<Expression> resolveIdentifierMaybe(@Nonnull Identifier identifier,
                                                        @Nonnull LogicalOperators operators) {
        var attributes = lookup(identifier, operators, true);
        Assert.thatUnchecked(attributes.size() <= 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        if (attributes.isEmpty()) {
            attributes = lookup(identifier, operators, false);
        }
        if (attributes.isEmpty()) {
            return Optional.empty();
        }
        Assert.thatUnchecked(attributes.size() == 1, ErrorCode.AMBIGUOUS_COLUMN, () -> String.format(Locale.ROOT, "Ambiguous reference %s", identifier));
        return Optional.of(attributes.get(0));
    }

    private static Optional<Expression> lookupPseudoField(@Nonnull LogicalOperator logicalOperator,
                                                          @Nonnull Identifier identifier,
                                                          boolean matchQualifiedOnly) {
        if (matchQualifiedOnly && (!identifier.isQualified() || logicalOperator.getName().isEmpty())) {
            return Optional.empty();
        }
        if (matchQualifiedOnly && identifier.isQualified() && identifier.fullyQualifiedName().size() != 2) {
            return Optional.empty();
        }
        if (!identifier.isQualified()) {
            return PseudoColumn.mapToExpressionMaybe(logicalOperator, identifier.getName());
        }
        if (logicalOperator.getName().isEmpty()) {
            return Optional.empty();
        }
        if (!identifier.prefixedWith(logicalOperator.getName().get())) {
            return Optional.empty();
        }
        return PseudoColumn.mapToExpressionMaybe(logicalOperator, identifier.getName());
    }

    @Nonnull
    private List<Expression> lookup(@Nonnull Identifier referenceIdentifier,
                                    @Nonnull LogicalOperators operators,
                                    boolean matchQualifiedOnly) {
        if (matchQualifiedOnly && !referenceIdentifier.isQualified()) {
            return ImmutableList.of();
        }
        final ImmutableList.Builder<Expression> matchedAttributes = ImmutableList.builder();
        for (final var operator : operators) {
            if (operator.getQuantifier() instanceof Quantifier.Existential) {
                continue;
            }
            final var operatorNameMaybe = operator.getName();
            boolean checkForPseudoColumns = true;
            for (final var attribute : operator.getOutput()) {
                if (!referenceIdentifier.isQualified() && attribute.hidden) {
                    continue;
                }
                if (attribute.getName().isEmpty()) {
                    continue;
                }
                final var attributeIdentifier = attribute.getName().get();
                if (attributeIdentifier.equals(referenceIdentifier)) {
                    matchedAttributes.add(attribute);
                    checkForPseudoColumns = false;
                    continue;
                } else if (!matchQualifiedOnly && attributeIdentifier.withoutQualifier().equals(referenceIdentifier)) {
                    matchedAttributes.add(attribute);
                    checkForPseudoColumns = false;
                    continue;
                }
                if (matchQualifiedOnly && operatorNameMaybe.isPresent()) {
                    if (attributeIdentifier.withQualifier(operatorNameMaybe.get().getName()).equals(referenceIdentifier)) {
                        matchedAttributes.add(attribute);
                        checkForPseudoColumns = false;
                        continue;
                    }
                }
                final var nestedFieldMaybe = lookupNestedField(referenceIdentifier, attribute, operator, matchQualifiedOnly);
                if (nestedFieldMaybe.isPresent()) {
                    matchedAttributes.add(nestedFieldMaybe.get());
                    checkForPseudoColumns = false;
                }
            }
            if (checkForPseudoColumns) {
                lookupPseudoField(operator, referenceIdentifier, matchQualifiedOnly)
                        .ifPresent(matchedAttributes::add);
            }
        }
        return matchedAttributes.build();
    }

    private List<Expression> lookupOneOperaror(@Nonnull Identifier referenceIdentifier,
                                    @Nonnull LogicalOperator operator,
                                    boolean matchQualifiedOnly) {
        if (matchQualifiedOnly && !referenceIdentifier.isQualified()) {
            return ImmutableList.of();
        }
        final ImmutableList.Builder<Expression> matchedAttributes = ImmutableList.builder();
        if (operator.getQuantifier() instanceof Quantifier.Existential) {
            return ImmutableList.of();
        }
        final var operatorNameMaybe = operator.getName();
        boolean checkForPseudoColumns = true;
        for (final var attribute : operator.getOutput()) {
            if (attribute.getName().isEmpty()) {
                continue;
            }
            final var attributeIdentifier = attribute.getName().get();
            if (attributeIdentifier.equals(referenceIdentifier)) {
                matchedAttributes.add(attribute);
                checkForPseudoColumns = false;
                continue;
            } else if (!matchQualifiedOnly && attributeIdentifier.withoutQualifier().equals(referenceIdentifier)) {
                matchedAttributes.add(attribute);
                checkForPseudoColumns = false;
                continue;
            }
            if (matchQualifiedOnly && operatorNameMaybe.isPresent()) {
                if (attributeIdentifier.withQualifier(operatorNameMaybe.get().getName()).equals(referenceIdentifier)) {
                    matchedAttributes.add(attribute);
                    checkForPseudoColumns = false;
                    continue;
                }
            }
            final var nestedFieldMaybe = lookupNestedField(referenceIdentifier, attribute, operator, matchQualifiedOnly);
            if (nestedFieldMaybe.isPresent()) {
                matchedAttributes.add(nestedFieldMaybe.get());
                checkForPseudoColumns = false;
            }
        }
        if (checkForPseudoColumns) {
            lookupPseudoField(operator, referenceIdentifier, matchQualifiedOnly)
                    .ifPresent(matchedAttributes::add);
        }
        return matchedAttributes.build();
    }

    @Nonnull
    public Optional<Expression> lookupAlias(@Nonnull Identifier requestedAlias,
                                            @Nonnull Expressions existingExpressions) {
        if (requestedAlias.isQualified()) {
            return Optional.empty();
        }
        final ImmutableList.Builder<Expression> matchedAttributesBuilder = ImmutableList.builder();
        for (final var expression : existingExpressions) {
            if (expression.getName().isEmpty()) {
                continue;
            }
            if (expression.getName().get().isQualified()) {
                continue;
            }
            if (expression.getName().get().equals(requestedAlias)) {
                matchedAttributesBuilder.add(expression);
            }
        }
        final var matchedAttributes = matchedAttributesBuilder.build();
        if (matchedAttributes.size() > 1) {
            Assert.failUnchecked(ErrorCode.AMBIGUOUS_COLUMN, String.format(Locale.ROOT, "Ambiguous alias %s", requestedAlias));
        }
        return matchedAttributes.isEmpty() ? Optional.empty() : Optional.of(matchedAttributes.get(0));
    }

    public Optional<Expression> lookupNestedField(@Nonnull Identifier requestedIdentifier,
                                                  @Nonnull Expression existingExpression,
                                                  @Nonnull LogicalOperator logicalOperator,
                                                  boolean matchQualifiedOnly) {
        final var effectiveExistingExpr = matchQualifiedOnly && logicalOperator.getName().isPresent() ?
                                          existingExpression.withQualifier(Optional.of(logicalOperator.getName().get())) :
                                          existingExpression.clearQualifier();
        return lookupNestedField(requestedIdentifier, existingExpression, effectiveExistingExpr, false);
    }

    @Nonnull
    public Optional<Expression> lookupNestedField(@Nonnull Identifier requestedIdentifier,
                                                  @Nonnull Expression existingExpression,
                                                  @Nonnull Expression effectiveExistingExpr,
                                                  boolean allowLookupRoot) {
        // if allow look up root and the requestedIdentifier is effectiveExistingExpr, return effectiveExistingExpr
        if (allowLookupRoot && requestedIdentifier.fullyQualifiedName().size() == effectiveExistingExpr.getName().get().fullyQualifiedName().size()) {
            return Optional.of(effectiveExistingExpr);
        }
        if (existingExpression.getName().isEmpty() || requestedIdentifier.fullyQualifiedName().size() <= 1) {
            return Optional.empty();
        }
        var effectiveExprName = effectiveExistingExpr.getName().orElseThrow();
        if (!requestedIdentifier.prefixedWith(effectiveExprName)) {
            /*
             * This is a special case where the expression is actually qualified while the containing logical operator
             * is not representing a rare case,
             * where a logical operator contains a mix of columns that are qualified differently.
             */
            if (existingExpression.getName().isPresent() &&
                    requestedIdentifier.prefixedWith(existingExpression.getName().get())) {
                effectiveExprName = existingExpression.getName().get();
            } else {
                return Optional.empty();
            }
        }
        final var remainingPath = requestedIdentifier.fullyQualifiedName().subList(effectiveExprName.fullyQualifiedName().size(), requestedIdentifier.fullyQualifiedName().size());
        if (remainingPath.isEmpty()) {
            return Optional.of(existingExpression.withName(requestedIdentifier));
        }
        final ImmutableList.Builder<FieldValue.Accessor> accessors = ImmutableList.builder();
        DataType type = existingExpression.getDataType();
        for (String s : remainingPath) {
            if (type.getCode() != DataType.Code.STRUCT) {
                return Optional.empty();
            }
            final var fields = ((DataType.StructType) type).getFields();
            var found = false;
            for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).getName().equals(s)) {
                    accessors.add(new FieldValue.Accessor(fields.get(j).getName(), j));
                    type = fields.get(j).getType();
                    found = true;
                    break;
                }
            }
            if (!found) {
                return Optional.empty();
            }
        }
        final var fieldPath = FieldValue.resolveFieldPath(existingExpression.getUnderlying().getResultType(), accessors.build());
        final var attributeExpression = FieldValue.ofFieldsAndFuseIfPossible(existingExpression.getUnderlying(), fieldPath);
        final var nestedAttribute = new Expression(Optional.of(requestedIdentifier), type, attributeExpression);
        return Optional.of(nestedAttribute);
    }

    public static final class ParsedTypeInfo {
        @Nullable
        private final RelationalParser.PrimitiveTypeContext primitiveTypeContext;

        @Nullable
        private final Identifier customType;

        private final boolean isNullable;

        private final boolean isRepeated;

        private ParsedTypeInfo(@Nullable final RelationalParser.PrimitiveTypeContext primitiveTypeContext,
                               @Nullable final Identifier customType, final boolean isNullable, final boolean isRepeated) {
            this.primitiveTypeContext = primitiveTypeContext;
            this.customType = customType;
            this.isNullable = isNullable;
            this.isRepeated = isRepeated;
        }

        public boolean hasPrimitiveType() {
            return primitiveTypeContext != null;
        }

        @Nullable
        public RelationalParser.PrimitiveTypeContext getPrimitiveTypeContext() {
            return primitiveTypeContext;
        }

        public boolean hasCustomType() {
            return customType != null;
        }

        @Nullable
        public Identifier getCustomType() {
            return customType;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public boolean isRepeated() {
            return isRepeated;
        }

        @Nonnull
        public static ParsedTypeInfo ofPrimitiveType(@Nonnull final RelationalParser.PrimitiveTypeContext primitiveTypeContext,
                                                     final boolean isNullable, final boolean isRepeated) {
            return new ParsedTypeInfo(primitiveTypeContext, null, isNullable, isRepeated);
        }

        @Nonnull
        public static ParsedTypeInfo ofCustomType(@Nonnull final Identifier customType,
                                                  final boolean isNullable, final boolean isRepeated) {
            return new ParsedTypeInfo(null, customType, isNullable, isRepeated);
        }
    }

    @Nonnull
    public DataType lookupBuiltInType(@Nonnull final ParsedTypeInfo parsedTypeInfo) {
        Assert.thatUnchecked(!parsedTypeInfo.hasCustomType(), ErrorCode.INTERNAL_ERROR, () -> "unexpected custom type " +
                Assert.notNullUnchecked(parsedTypeInfo.getCustomType()).getName());
        return lookupType(parsedTypeInfo, typeToLookUp -> {
            Assert.failUnchecked("unexpected custom type " + typeToLookUp);
            return Optional.empty();
        });
    }

    @Nonnull
    public DataType lookupType(@Nonnull final ParsedTypeInfo parsedTypeInfo,
                               @Nonnull final Function<String, Optional<DataType>> dataTypeProvider) {
        DataType type;
        final var isNullable = parsedTypeInfo.isNullable();
        if (parsedTypeInfo.hasCustomType()) {
            final var typeName = Assert.notNullUnchecked(parsedTypeInfo.getCustomType()).getName();
            final var maybeFound = dataTypeProvider.apply(typeName);
            // if we cannot find the type now, mark it, we will try to resolve it later on via a second pass.
            type = maybeFound.map(dataType -> dataType.withNullable(isNullable)).orElseGet(() -> DataType.UnresolvedType.of(typeName, isNullable));
        } else {
            final var primitiveType = Assert.notNullUnchecked(parsedTypeInfo.getPrimitiveTypeContext());
            if (primitiveType.vectorType() != null) {
                final var vectorTypeCtx = primitiveType.vectorType();
                final var vectorElementTypeCtx = vectorTypeCtx.elementType;
                int precision = 16;
                if (vectorElementTypeCtx.FLOAT() != null) {
                    precision = 32;
                } else if (vectorElementTypeCtx.DOUBLE() != null) {
                    precision = 64;
                } else {
                    Assert.notNullUnchecked(vectorElementTypeCtx.HALF(), ErrorCode.SYNTAX_ERROR, "unsupported vector element type " + vectorTypeCtx.getText());
                }
                int length = Assert.castUnchecked(ParseHelpers.parseDecimal(vectorTypeCtx.dimensions.getText()), Integer.class);
                Assert.thatUnchecked(length > 0, ErrorCode.SYNTAX_ERROR, "vector dimension must be positive");
                type = DataType.VectorType.of(precision, length, isNullable);
            } else {
                final var primitiveTypeName = parsedTypeInfo.getPrimitiveTypeContext().getText();

                switch (primitiveTypeName.toUpperCase(Locale.ROOT)) {
                    case "STRING":
                        type = isNullable ? DataType.Primitives.NULLABLE_STRING.type() : DataType.Primitives.STRING.type();
                        break;
                    case "INTEGER":
                        type = isNullable ? DataType.Primitives.NULLABLE_INTEGER.type() : DataType.Primitives.INTEGER.type();
                        break;
                    case "BIGINT":
                        type = isNullable ? DataType.Primitives.NULLABLE_LONG.type() : DataType.Primitives.LONG.type();
                        break;
                    case "DOUBLE":
                        type = isNullable ? DataType.Primitives.NULLABLE_DOUBLE.type() : DataType.Primitives.DOUBLE.type();
                        break;
                    case "BOOLEAN":
                        type = isNullable ? DataType.Primitives.NULLABLE_BOOLEAN.type() : DataType.Primitives.BOOLEAN.type();
                        break;
                    case "BYTES":
                        type = isNullable ? DataType.Primitives.NULLABLE_BYTES.type() : DataType.Primitives.BYTES.type();
                        break;
                    case "FLOAT":
                        type = isNullable ? DataType.Primitives.NULLABLE_FLOAT.type() : DataType.Primitives.FLOAT.type();
                        break;
                    case "UUID":
                        type = isNullable ? DataType.Primitives.NULLABLE_UUID.type() : DataType.Primitives.UUID.type();
                        break;
                    default:
                        Assert.notNullUnchecked(metadataCatalog);
                        // assume it is a custom type, will fail in upper layers if the type can not be resolved.
                        // lookup the type (Struct, Table, or Enum) in the schema template metadata under construction.
                        final var maybeFound = dataTypeProvider.apply(primitiveTypeName);
                        // if we cannot find the type now, mark it, we will try to resolve it later on via a second pass.
                        type = maybeFound.orElseGet(() -> DataType.UnresolvedType.of(primitiveTypeName, isNullable));
                        break;
                }
            }
        }

        if (parsedTypeInfo.isRepeated()) {
            return DataType.ArrayType.from(type.withNullable(false), isNullable);
        } else {
            return type;
        }
    }

    @Nonnull
    private static Expressions expandStructExpression(@Nonnull Expression expression) {
        Assert.thatUnchecked(expression.getDataType().getCode() == DataType.Code.STRUCT, ErrorCode.INVALID_COLUMN_REFERENCE,
                () -> String.format(Locale.ROOT, "attempt to expand non-struct expression %s", expression));
        final ImmutableList.Builder<Expression> resultBuilder = ImmutableList.builder();
        final var underlying = expression.getUnderlying();
        final var type = Assert.castUnchecked(expression.getDataType(), DataType.StructType.class);
        var colCount = 0;
        for (final var field : type.getFields()) {
            final var expandedExpressionValue = FieldValue.ofOrdinalNumber(underlying, colCount);
            final var expandedExpressionQuantifier = expression.getName().map(Identifier::fullyQualifiedName).orElse(ImmutableList.of());
            final var expandedExpressionName = field.getName();
            final var expandedExpressionType = DataTypeUtils.toRelationalType(expandedExpressionValue.getResultType());
            resultBuilder.add(new Expression(Optional.of(Identifier.of(expandedExpressionName, expandedExpressionQuantifier)),
                    expandedExpressionType, expandedExpressionValue));
            colCount++;
        }
        return Expressions.of(resultBuilder.build());
    }

    public void validateInListItems(@Nonnull Expressions inListItems) {
        for (final var inListItem : inListItems) {
            final var resultType = inListItem.getUnderlying().getResultType();
            Assert.thatUnchecked(resultType != Type.NULL, ErrorCode.WRONG_OBJECT_TYPE, "NULL values are not allowed in the IN list");
            Assert.thatUnchecked(!resultType.isUnresolved(), ErrorCode.UNKNOWN_TYPE,  String.format(Locale.ROOT, "Type cannot be determined for element `%s` in the IN list", inListItem));
        }
    }

    public static void validateGroupByAggregates(@Nonnull Expressions groupByExpressions) {
        final var nestedAggregates = groupByExpressions.stream()
                .filter(expression -> expression.getUnderlying() instanceof AggregateValue)
                .filter(agg -> agg.getUnderlying().preOrderStream().skip(1).anyMatch(c -> c instanceof StreamableAggregateValue || c instanceof IndexableAggregateValue))
                .collect(ImmutableSet.toImmutableSet());
        Assert.thatUnchecked(nestedAggregates.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION, () -> String.format(Locale.ROOT, "unsupported nested aggregate(s) %s",
                nestedAggregates.stream().map(ex -> ex.getUnderlying().toString()).collect(Collectors.joining(","))));
    }

    /**
     * Checks that the logical operator forming the legs of a {@code UNION} have compatible types. The rules are:
     * <ul>
     *     <li>each leg must project the same number of columns</li>
     *     <li>the ith columns of each union leg have either the same type or have a common type promotion, in other
     *     words, their maximum type is well-defined.
     * </ul>
     * @param unionLegs The logical operators representing the legs of the union.
     * @return If all union legs have the same type, returns {@code Optional.empty()}, otherwise, returns a {@code Type.Record}
     * representing the maximum type of each column calculated pairwise.
     */
    @Nonnull
    public static Optional<Type.Record> validateUnionTypes(@Nonnull LogicalOperators unionLegs) {
        final var distinctTypesCount = unionLegs.stream().map(exp -> exp.getOutput().expanded().size()).distinct().count();
        Assert.thatUnchecked(distinctTypesCount == 1, ErrorCode.UNION_INCORRECT_COLUMN_COUNT,
                "UNION legs do not have the same number of columns");
        Type.Record result = null;
        boolean requiresPromotion = false;
        for (final var unionLegType : unionLegs.stream().map(leg -> leg.getQuantifier().getFlowedObjectType())
                .map(type -> Assert.castUnchecked(type, Type.Record.class)).collect(ImmutableList.toImmutableList())) {
            if (result == null) {
                result = unionLegType;
                continue;
            }
            if (requiresPromotion) {
                result = Assert.castUnchecked(Type.maximumType(result, unionLegType), Type.Record.class);
                continue;
            }
            final var oldType = result;
            result = Assert.castUnchecked(Assert.notNullUnchecked(Type.maximumType(result, unionLegType), ErrorCode.UNION_INCOMPATIBLE_COLUMNS,
                            "Incompatible column types in UNION legs"),
                    Type.Record.class);
            if (!oldType.equals(result)) {
                requiresPromotion = true;
            }
        }
        return requiresPromotion ? Optional.of(Assert.notNullUnchecked(result)) : Optional.empty();
    }

    @Nonnull
    public Type.Array resolveArrayTypeFromValues(@Nonnull Expressions arrayItems) {
        final var arrayItemsTypes = Streams
                .stream(arrayItems)
                .map(Expression::getUnderlying)
                .map(Value::getResultType)
                .collect(ImmutableList.toImmutableList());
        return resolveArrayTypeFromElementTypes(arrayItemsTypes);
    }

    public static boolean isComposableFrom(@Nonnull Expression expression,
                                           @Nonnull Expressions parts,
                                           @Nonnull AliasMap aliasMap,
                                           @Nonnull Set<CorrelationIdentifier> constantCorrelations) {
        final Correlated.BoundEquivalence<Value> boundEquivalence = new Correlated.BoundEquivalence<>(aliasMap);
        final var boundParts = Streams.stream(parts).map(Expression::getUnderlying).map(boundEquivalence::wrap)
                .collect(ImmutableSet.toImmutableSet());
        return isComposableFromInternal(expression.getUnderlying(), boundParts, boundEquivalence, constantCorrelations);
    }

    private static boolean isComposableFromInternal(@Nonnull Value value,
                                                    @Nonnull Set<Equivalence.Wrapper<Value>> parts,
                                                    @Nonnull Correlated.BoundEquivalence<Value> boundEquivalence,
                                                    @Nonnull Set<CorrelationIdentifier> constantCorrelations) {
        final var boundValue = boundEquivalence.wrap(value);
        if (parts.contains(boundValue)) {
            return true;
        }
        if (value.isConstant()) {
            return true;
        }
        if (constantCorrelations.containsAll(value.getCorrelatedTo())) {
            return true;
        }
        if (value instanceof ArithmeticValue ||
                value instanceof RecordConstructorValue ||
                value instanceof RelOpValue.BinaryRelOpValue ||
                value instanceof AndOrValue ||
                value instanceof NotValue ||
                value instanceof InOpValue) {
            for (final var child : value.getChildren()) {
                if (!isComposableFromInternal(child, parts, boundEquivalence, constantCorrelations)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Nonnull
    private static Type.Array resolveArrayTypeFromElementTypes(@Nonnull List<Type> types) {
        Type elementType;
        if (types.isEmpty()) {
            elementType = Type.nullType();
        } else {
            // all values must have the same type.
            final var distinctTypes = types.stream().filter(type -> type != Type.nullType()).distinct().collect(Collectors.toList());
            Assert.thatUnchecked(distinctTypes.size() == 1, ErrorCode.DATATYPE_MISMATCH, "could not determine type of constant array");
            elementType = distinctTypes.get(0);
        }
        return new Type.Array(elementType);
    }

    /**
     * validates that a SQL {@code LIMIT} expression is within allowed limits of {@code [1, Integer.MAX_VALUE]}.
     * @param expression The {@code LIMIT} literal expression.
     */
    public static void validateLimit(@Nonnull Expression expression) {
        final long minInclusive = 1;
        final long maxInclusive = Integer.MAX_VALUE;
        final var underlying = expression.getUnderlying();
        Assert.thatUnchecked(underlying instanceof LiteralValue<?>);
        final var value = ((LiteralValue<?>) underlying).getLiteralValue();
        Assert.notNullUnchecked(value, ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
                () -> String.format(Locale.ROOT, "limit value out of range [1, %d]", Integer.MAX_VALUE));
        if (value.getClass() == Integer.class) {
            Assert.thatUnchecked(minInclusive <= ((Integer) value) && ((Integer) value) <= maxInclusive,
                    ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
                    () -> String.format(Locale.ROOT, "limit value out of range [1, %d]", Integer.MAX_VALUE));
            return;
        }
        if (value.getClass() == Long.class) {
            Assert.thatUnchecked(minInclusive <= ((Long) value) && ((Long) value) <= maxInclusive,
                    ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
                    () -> String.format(Locale.ROOT, "limit value out of range [1, %d]", Integer.MAX_VALUE));
            return;
        }
        Assert.failUnchecked("unexpected limit type " + value.getClass());
    }

    public static void validateDatabaseUri(@Nonnull Identifier path) {
        Assert.thatUnchecked(Objects.requireNonNull(path.getName()).matches("/\\w[a-zA-Z0-9_/]*\\w"),
                ErrorCode.INVALID_PATH, () -> String.format(Locale.ROOT, "invalid database path '%s'", path));
    }

    public static void validateCteColumnAliases(@Nonnull LogicalOperator logicalOperator, @Nonnull List<Identifier> columnAliases) {
        final var expressions = logicalOperator.getOutput().expanded();
        Assert.thatUnchecked(expressions.size() == columnAliases.size(), ErrorCode.INVALID_COLUMN_REFERENCE,
                () -> String.format(Locale.ROOT, "cte query has %d column(s), however %d aliases defined", expressions.size(), columnAliases.size()));
    }

    @Nonnull
    public static NonnullPair<Optional<URI>, String> parseSchemaIdentifier(@Nonnull final Identifier schemaIdentifier) {
        final var id = schemaIdentifier.getName();
        Assert.notNullUnchecked(id);
        if (id.startsWith("/")) {
            validateDatabaseUri(schemaIdentifier);
            int separatorIdx = id.lastIndexOf("/");
            Assert.thatUnchecked(separatorIdx < id.length() - 1);
            return NonnullPair.of(Optional.of(URI.create(id.substring(0, separatorIdx))), id.substring(separatorIdx + 1));
        } else {
            return NonnullPair.of(Optional.empty(), id);
        }
    }

    public static void validateContinuation(@Nonnull final Expression continuation) {
        // currently, this only validates that the underlying Value is a byte string.
        // in the future, we might add more context-aware checks.
        final var underlying = continuation.getUnderlying();
        Assert.thatUnchecked(underlying instanceof LiteralValue<?>, ErrorCode.INVALID_CONTINUATION,
                "Unexpected continuation parameter of type %s", underlying.getClass().getSimpleName());
        final var continuationBytes = Assert.castUnchecked(underlying, LiteralValue.class).getLiteralValue();
        Assert.notNullUnchecked(continuationBytes);
        Assert.thatUnchecked(continuationBytes instanceof ByteString, ErrorCode.INVALID_CONTINUATION,
                "Unexpected continuation parameter of type %s", continuationBytes.getClass().getSimpleName());
    }

    /**
     * Resolves a scalar function given its name and a list of arguments by looking it up in the
     * {@link SqlFunctionCatalog}.
     * <br>
     * Ideally, this overload should not exist, in other words, the caller should not be responsible for determining
     * whether the single-item records should be flattened or not.
     * Currently almost all supported SQL functions do not expect {@code Record} objects,
     * so this is probably ok, however, this does not necessarily hold for the future.
     * See {@link SqlFunctionCatalog#flattenRecordWithOneField(Typed)} for more information.
     *
     * @param functionName The function name.
     * @param arguments The function arguments.
     * @param flattenSingleItemRecords {@code true} if single-item records should be (recursively) replaced with their
     * content, otherwise {@code false}.
     *
     * @return An {@link Expression} representing the resolved SQL function.
     */
    @Nonnull
    public Expression resolveScalarFunction(@Nonnull final String functionName, @Nonnull final Expressions arguments,
                                            boolean flattenSingleItemRecords) {
        Assert.thatUnchecked(functionCatalog.containsFunction(functionName), ErrorCode.UNSUPPORTED_QUERY,
                () -> String.format(Locale.ROOT, "Unsupported operator %s", functionName));

        final var builtInFunction = functionCatalog.lookupFunction(functionName, arguments);
        processFunctionSideEffects(builtInFunction);

        final var argumentList = ImmutableList.<Expression>builderWithExpectedSize(arguments.size() + 1).addAll(arguments);
        if (BITMAP_SCALAR_FUNCTIONS.contains(functionName.toLowerCase(Locale.ROOT))) {
            argumentList.add(Expression.ofUnnamed(new LiteralValue<>(BITMAP_DEFAULT_ENTRY_SIZE)));
        }

        final List<? extends Typed> valueArgs = argumentList.build().stream().map(Expression::getUnderlying)
                .map(v -> flattenSingleItemRecords ? SqlFunctionCatalog.flattenRecordWithOneField(v) : v)
                .collect(ImmutableList.toImmutableList());
        final var resultingValue = Assert.castUnchecked(builtInFunction.encapsulate(valueArgs), Value.class);
        return Expression.ofUnnamed(DataTypeUtils.toRelationalType(resultingValue.getResultType()), resultingValue);
    }

    private void processFunctionSideEffects(@Nonnull final CatalogedFunction builtInFunction) {
        if (!(builtInFunction instanceof WithPlanGenerationSideEffects)) {
            return;
        }
        // combine the expanded function's literals (if any) with the currently built list of query literals.
        mutablePlanGenerationContext.importAuxiliaryLiterals(Assert.castUnchecked(builtInFunction,
                WithPlanGenerationSideEffects.class).getAuxiliaryLiterals());
    }

    /**
     * Resolves a table function given its name and a list of arguments by looking it up in the
     * {@link SqlFunctionCatalog}.
     * <br>
     * Ideally, this overload should not exist, in other words, the caller should not be responsible for determining
     * whether the single-item records should be flattened or not.
     * Currently almost all supported SQL functions do not expect {@code Record} objects,
     * so this is probably ok, however, this does not necessarily hold for the future.
     * See {@link SqlFunctionCatalog#flattenRecordWithOneField(Typed)} for more information.
     *
     * @param functionName The function name.
     * @param arguments The function arguments.
     * @param flattenSingleItemRecords {@code true} if single-item records should be (recursively) replaced with their
     * content, otherwise {@code false}.
     *
     * @return A {@link LogicalOperator} representing the semantics of the requested SQL table function.
     */
    @Nonnull
    public LogicalOperator resolveTableFunction(@Nonnull final Identifier functionName, @Nonnull final Expressions arguments,
                                                boolean flattenSingleItemRecords) {
        Assert.thatUnchecked(functionCatalog.containsFunction(functionName.getName()), ErrorCode.UNDEFINED_FUNCTION,
                () -> String.format(Locale.ROOT, "Unknown function %s", functionName));
        final var tableFunction = functionCatalog.lookupFunction(functionName.getName(), arguments);
        if (tableFunction instanceof BuiltInFunction) {
            Assert.thatUnchecked(tableFunction instanceof BuiltInTableFunction, functionName + " is not a table-valued function");
        }
        processFunctionSideEffects(tableFunction);

        final List<? extends Typed> valueArgs = Streams.stream(arguments.underlying().iterator())
                .map(v -> flattenSingleItemRecords ? SqlFunctionCatalog.flattenRecordWithOneField(v) : v)
                .collect(ImmutableList.toImmutableList());
        Assert.thatUnchecked(arguments.allNamedArguments() || arguments.noneNamedArguments(), ErrorCode.UNSUPPORTED_OPERATION,
                "mixing named and unnamed arguments is not supported");

        final var resultingValue = arguments.allNamedArguments()
                ? tableFunction.encapsulate(arguments.toNamedArgumentInvocation())
                : tableFunction.encapsulate(valueArgs);
        if (resultingValue instanceof StreamingValue) {
            final var tableFunctionExpression = new TableFunctionExpression(Assert.castUnchecked(resultingValue, StreamingValue.class));
            final var reference = Reference.initialOf(tableFunctionExpression);
            final var resultingQuantifier = Quantifier.forEach(reference);
            final var output = Expressions.of(LogicalOperator.convertToExpressions(resultingQuantifier));
            return LogicalOperator.newNamedOperator(functionName, output, resultingQuantifier);
        }
        final var tableExpression = Assert.castUnchecked(resultingValue, RelationalExpression.class);
        final var topQun = Quantifier.forEach(Reference.initialOf(tableExpression));
        return LogicalOperator.newNamedOperator(functionName, Expressions.fromQuantifier(topQun), topQun);
    }

    @Nonnull
    public LogicalOperator resolveView(@Nonnull final Identifier viewIdentifier) {
        final View view;
        try {
            view = Assert.optionalUnchecked(metadataCatalog.findViewByName(viewIdentifier.getName()));
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
        final var recordLayerView = Assert.castUnchecked(view, RecordLayerView.class);
        return recordLayerView.getCompilableViewSupplier().apply(isCaseSensitive);
    }

    // TODO: this will be removed once we unify both Java- and SQL-UDFs.
    public boolean isJavaCallFunction(@Nonnull final String functionName) {
        return functionCatalog.isJavaCallFunction(functionName);
    }

    public boolean containsReferencesTo(@Nonnull final ParseTree parseTree,
                                        @Nonnull final Identifier identifier,
                                        @Nonnull final Function<RelationalParser.FullIdContext, Identifier> idParser) {
        return ParseHelpers.ParseTreeLikeAdapter.from(parseTree).preOrderStream()
                .map(ParseHelpers.ParseTreeLikeAdapter::getParseTree)
                .anyMatch(child -> (child instanceof RelationalParser.TableNameContext)
                        && idParser.apply(((RelationalParser.TableNameContext) child).fullId()).equals(identifier));
    }

    /**
     * Infers the type of recursive query.
     *
     * @param namedQueryBody the recursive query.
     * @param queryName the query name.
     * @param idParser A parser of IDs.
     * @param memoizer A logical operator parsing memoizer.
     * @param queryVisitor A query visitor instance.
     * @return the type of recursive query, as identified by the type of the first non-recursive branch.
     */
    @Nonnull
    public Optional<Type> getRecursiveCteType(@Nonnull final RelationalParser.QueryContext namedQueryBody,
                                              @Nonnull final Identifier queryName,
                                              @Nonnull final Function<RelationalParser.FullIdContext, Identifier> idParser,
                                              @Nonnull final Function<ParserRuleContext, LogicalOperators> memoizer,
                                              @Nonnull final QueryVisitor queryVisitor) {
        final AtomicReference<Optional<Type>> result = new AtomicReference<>(Optional.empty());
        recursiveQueryTraversal(namedQueryBody, queryName, idParser,
                nonRecursiveBranch -> {
                    if (result.get().isEmpty()) {
                        final var logicalOperator = handleQueryFragment(nonRecursiveBranch, namedQueryBody, memoizer, queryVisitor);
                        result.set(Optional.of(logicalOperator.getQuantifier().getFlowedObjectType()));
                    }
                },
                ignored -> {
                });
        return result.get();
    }

    /**
     * Parses and partitions recursive query branches into two groups: non-recursive and recursive branches.
     * Note that this will be rewritten once Relational has a semantic analysis phase that is able to deduce
     *
     * @param namedQueryBody The recursive query.
     * @param queryName The recursive query name.
     * @param idParser A parser of IDs.
     * @param memoizer A logical operator parsing memoizer.
     * @param queryVisitor A query visitor instance.
     * @return A partitioning of recursive query into a list of non-recursive logical operators and list of recursive logical operators.
     */
    @Nonnull
    public NonnullPair<List<LogicalOperator>, List<LogicalOperator>> partitionRecursiveQuery(@Nonnull final RelationalParser.QueryContext namedQueryBody,
                                                                                             @Nonnull final Identifier queryName,
                                                                                             @Nonnull final Function<RelationalParser.FullIdContext, Identifier> idParser,
                                                                                             @Nonnull final Function<ParserRuleContext, LogicalOperators> memoizer,
                                                                                             @Nonnull final QueryVisitor queryVisitor) {
        final var nonRecursiveBranchesBuilder = ImmutableList.<LogicalOperator>builder();
        final var recursiveBranchesBuilder = ImmutableList.<LogicalOperator>builder();

        recursiveQueryTraversal(namedQueryBody, queryName, idParser,
                nonRecursiveBranch -> {
                    final var logicalOperator = handleQueryFragment(nonRecursiveBranch, namedQueryBody, memoizer, queryVisitor);
                    nonRecursiveBranchesBuilder.add(logicalOperator);
                },
                recursiveBranch -> {
                    final var logicalOperator = handleQueryFragment(recursiveBranch, namedQueryBody, memoizer, queryVisitor);
                    recursiveBranchesBuilder.add(logicalOperator);
                });
        return NonnullPair.of(nonRecursiveBranchesBuilder.build(), recursiveBranchesBuilder.build());
    }

    private void recursiveQueryTraversal(@Nonnull final RelationalParser.QueryContext namedQueryBody,
                                         @Nonnull final Identifier queryName,
                                         @Nonnull final Function<RelationalParser.FullIdContext, Identifier> idParser,
                                         @Nonnull final Consumer<ParserRuleContext> nonRecursiveBranchConsumer,
                                         @Nonnull final Consumer<ParserRuleContext> recursiveBranchConsumer) {
        final var hasNestedCtes = namedQueryBody.ctes() != null;
        if (hasNestedCtes) {
            for (final var nestedNamedQuery : namedQueryBody.ctes().namedQuery()) {
                final var nestedQueryName = idParser.apply(nestedNamedQuery.name);
                Assert.thatUnchecked(!queryName.equals(nestedQueryName), ErrorCode.UNSUPPORTED_QUERY,
                        "ambiguous nested recursive CTE name");
            }
        }
        // look for the first non-recursive branch, parse it, and return its type.
        final var queryExpressionBody = namedQueryBody.queryExpressionBody();
        if (queryExpressionBody instanceof RelationalParser.QueryTermDefaultContext) {
            final var queryTerm = ((RelationalParser.QueryTermDefaultContext) queryExpressionBody).queryTerm();
            if (queryTerm instanceof RelationalParser.ParenthesisQueryContext) {
                // un-nest
                recursiveQueryTraversal(((RelationalParser.ParenthesisQueryContext) queryTerm).query(), queryName, idParser,
                        nonRecursiveBranchConsumer, recursiveBranchConsumer);
                return;
            }
            final var isRecursive = containsReferencesTo(queryTerm, queryName, idParser);
            if (isRecursive) {
                recursiveBranchConsumer.accept(queryTerm);
            } else {
                nonRecursiveBranchConsumer.accept(queryTerm);
            }
        } else {
            Assert.thatUnchecked(queryExpressionBody instanceof RelationalParser.SetQueryContext);
            final var setQueryContext = (RelationalParser.SetQueryContext) queryExpressionBody;
            // visit union's left branch
            recursiveQueryTraversal(
                    new RelationalParser.QueryContext((ParserRuleContext) namedQueryBody.parent, namedQueryBody.invokingState) {
                        @Override
                        public RelationalParser.QueryExpressionBodyContext queryExpressionBody() {
                            return setQueryContext.left;
                        }

                        @Override
                        public int getChildCount() {
                            return 1;
                        }

                        @Override
                        public ParseTree getChild(int i) {
                            Assert.thatUnchecked(i == 0);
                            return setQueryContext.left;
                        }
                    },
                    queryName,
                    idParser,
                    nonRecursiveBranchConsumer,
                    recursiveBranchConsumer);
            // visit union's right branch
            recursiveQueryTraversal(
                    new RelationalParser.QueryContext((ParserRuleContext) namedQueryBody.parent, namedQueryBody.invokingState) {
                        @Override
                        public RelationalParser.QueryExpressionBodyContext queryExpressionBody() {
                            return setQueryContext.right;
                        }

                        @Override
                        public int getChildCount() {
                            return 1;
                        }

                        @Override
                        public ParseTree getChild(int i) {
                            Assert.thatUnchecked(i == 0);
                            return setQueryContext.right;
                        }
                    },
                    queryName,
                    idParser,
                    nonRecursiveBranchConsumer,
                    recursiveBranchConsumer);
        }
    }

    @Nonnull
    private static LogicalOperator handleQueryFragment(@Nonnull final ParserRuleContext queryFragment,
                                                       @Nonnull final RelationalParser.QueryContext namedQueryBody,
                                                       @Nonnull final Function<ParserRuleContext, LogicalOperators> memoizer,
                                                       @Nonnull final QueryVisitor queryVisitor) {
        LogicalOperator logicalOperator;
        if (namedQueryBody.ctes() != null) {
            final var ctes = namedQueryBody.ctes();
            final var currentPlanFragment = queryVisitor.getDelegate().pushPlanFragment();
            memoizer.apply(ctes).forEach(currentPlanFragment::addOperator);
            logicalOperator = memoizer.apply(queryFragment).first();
            queryVisitor.getDelegate().popPlanFragment();
        } else {
            logicalOperator = memoizer.apply(queryFragment).first();
        }
        return logicalOperator;
    }
}
