/*
 * ParserUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.tuple.Pair;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Contains a set of utility methods that are relevant for parsing the AST.
 */
public final class ParserUtils {

    private static final Map<String, BuiltInFunction<? extends Value>> functionMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private ParserUtils() {
    }

    /**
     * If a string is single- or double-quoted, removes the quotation, otherwise, upper-case it.
     * @param string The input string
     * @return the normalized string
     */
    @Nullable
    public static String normalizeString(@Nullable final String string) {
        if (string == null) {
            return null;
        }
        if (isQuoted(string, "'") || isQuoted(string, "\"")) {
            return string.substring(1, string.length() - 1);
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

    static {
        functionMap.put("=", new RelOpValue.EqualsFn());
        functionMap.put(">", new RelOpValue.GtFn());
        functionMap.put("<", new RelOpValue.LtFn());
        functionMap.put("<=", new RelOpValue.LteFn());
        functionMap.put(">=", new RelOpValue.GteFn());
        functionMap.put("<>", new RelOpValue.NotEqualsFn());
        functionMap.put("!=", new RelOpValue.NotEqualsFn());

        functionMap.put("*", new ArithmeticValue.MulFn());
        functionMap.put("/", new ArithmeticValue.DivFn());
        functionMap.put("DIV", new ArithmeticValue.DivFn()); // should be case-insensitive
        functionMap.put("%", new ArithmeticValue.ModFn());
        functionMap.put("MOD", new ArithmeticValue.ModFn()); // should be case-insensitive
        functionMap.put("+", new ArithmeticValue.AddFn());
        functionMap.put("-", new ArithmeticValue.SubFn());

        functionMap.put("MAX", new NumericAggregationValue.MaxFn());
        functionMap.put("MIN", new NumericAggregationValue.MinFn());
        functionMap.put("AVG", new NumericAggregationValue.AvgFn());
        functionMap.put("SUM", new NumericAggregationValue.SumFn());
        functionMap.put("COUNT", new CountValue.CountFn());
        functionMap.put("COUNT*", new CountValue.CountStarFn()); // small hack done here, should be fixed in RL.
    }

    @Nonnull
    public static BuiltInFunction<? extends Value> getFunction(@Nonnull final String functionName) {
        BuiltInFunction<? extends Value> result = functionMap.get(functionName);
        Assert.notNullUnchecked(result, String.format("unsupported function '%s'", functionName));
        return result;
    }

    /**
     * Attempt to parse an input value into the corresponding numerical literal {@link Value}.
     * @param valueAsString The value to parse.
     * @param makeNegative <code>true</code> if the result should be negative, otherwise <code>false</code>.
     * @return The corresponding typed and literal {@link Value} object
     */
    @Nonnull
    public static Value parseDecimal(@Nonnull final String valueAsString, boolean makeNegative) {
        if (valueAsString.contains(".")) {
            double result = Double.parseDouble(valueAsString);
            if (makeNegative) {
                result *= -1;
            }
            return new LiteralValue<>(result);
        } else {
            long result = Long.parseLong(valueAsString);
            if (makeNegative) {
                result *= -1;
            }
            //TODO(bfines) I'm temporarily commenting this out to address TODO , but it almost
            //certainly isn't right to remove, so I'm leaving the commented code here so it can be easily restored
            //when someone who knows the planner type system can resolve the issue.
//            if (Integer.MIN_VALUE <= result && result <= Integer.MAX_VALUE) {
//                return new LiteralValue<>((int) result);
//            } else {
            return new LiteralValue<>(result);
            //            }
        }
    }

    public static FieldValue resolveField(@Nonnull final List<String> fieldPath, @Nonnull final ParserContext parserContext) {
        Assert.thatUnchecked(!fieldPath.isEmpty());
        final var isUnderlyingSelectWhere = parserContext.getCurrentScope().isFlagSet(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE);
        if (isUnderlyingSelectWhere) {
            return resolveFieldGroupedQuantifier(fieldPath, parserContext);
        } else {
            return resolveFieldSimpleQuantifier(fieldPath, parserContext);
        }
    }

    @Nonnull
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    private static FieldValue resolveFieldGroupedQuantifier(@Nonnull final List<String> fieldPath, @Nonnull final ParserContext parserContext) {
        Assert.thatUnchecked(!fieldPath.isEmpty());
        final var scope = parserContext.getCurrentScope();
        FieldValue result = null;
        var fieldAccessors = toAccessors(fieldPath);
        final var isResolvingAggregations = parserContext.getCurrentScope().isFlagSet(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
        final var isResolvingSelectHaving = parserContext.getCurrentScope().isFlagSet(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING);
        final var fieldPathStr = String.join(".", fieldPath);

        // Try to resolve the field by looking into the visible part of the only quantifier we have.
        Assert.thatUnchecked(scope.getForEachQuantifiers().size() == 1);
        final var qun = scope.getForEachQuantifiers().get(0);
        final var types = splitSelectWhereType(qun.getFlowedObjectType());

        // todo: check qualified columns.
        final var visibleType = isResolvingAggregations ? types.getRight() : types.getLeft();
        if (resolveFieldPath(visibleType, fieldAccessors)) {
            result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), fieldPath);
        }

        final var flownColumnsSplit = splitSelectWhereFlownColumns(qun);
        final var visibleColumns = isResolvingAggregations ? flownColumnsSplit.getRight() : flownColumnsSplit.getLeft();
        for (final var visibleField : visibleColumns.stream().map(Column::getField).collect(Collectors.toList())) {
            if (resolveFieldPath(visibleField.getFieldType(), fieldAccessors)) {
                Assert.isNullUnchecked(result, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
                result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.<String>builder().add(visibleField.getFieldName()).addAll(fieldPath).build());
            }
        }

        if (result != null) {
            return result;
        }

        // corner case:
        if (!isResolvingAggregations && fieldPath.size() > 1) {
            for (final var groupingColumnField : visibleColumns.stream().map(Column::getField).collect(Collectors.toList())) {
                if ((isResolvingSelectHaving || resolveFieldPath(types.getRight(), fieldAccessors)) && resolveFieldPath(groupingColumnField.getFieldType(), fieldAccessors.subList(1, fieldAccessors.size()))) {
                    Assert.isNullUnchecked(result, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
                    result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.<String>builder().add(visibleColumns.get(0).getField().getFieldName()).addAll(fieldPath.subList(1, fieldPath.size())).build());
                }
            }
        }

        if (result != null) {
            return result;
        }

        // We have not found the field in current scope, look into parent scopes for potential matches
        // because identifiers inside or outside an aggregation could in principles be correlated, and they
        // should be accepted since they are effectively constants within the subquery context:
        // select * from t where exists (select max(t.col1) from t2 group by t2.col2);
        {
            final var ancestorQuns = collectQuantifiersFromAncestorBlocks(scope);
            final var resolved = resolveField(ancestorQuns, fieldAccessors, fieldPath);
            Assert.thatUnchecked(resolved.size() <= 1, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
            if (resolved.size() == 1) {
                return resolved.get(0);
            }
        }

        Assert.failUnchecked(String.format("could not find field name '%s' in the grouping list", fieldPathStr), ErrorCode.GROUPING_ERROR);
        return null;
    }

    @Nonnull
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    private static FieldValue resolveFieldSimpleQuantifier(@Nonnull final List<String> fieldPath, @Nonnull final ParserContext parserContext) {
        final var scope = parserContext.getCurrentScope();
        final var fieldAccessors = toAccessors(fieldPath);
        final var fieldPathStr = String.join(".", fieldPath);

        // Try to resolve the field by looking into all quantifiers in current scope, if exactly one field is found, return it.
        // precedence resolves potential ambiguity with similarly named fields in top levels, i.e. do not look for ambiguity issues
        // if we already find a matching field in current scope.
        {
            final var resolved = resolveField(scope.getForEachQuantifiers(), fieldAccessors, fieldPath);
            Assert.thatUnchecked(resolved.size() <= 1, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
            if (resolved.size() == 1) {
                return resolved.get(0);
            }
        }

        // We have not found the field in current scope, look into parent scopes for potential matches
        {
            final var ancestorQuns = collectQuantifiersFromAncestorBlocks(scope);
            final var resolved = resolveField(ancestorQuns, fieldAccessors, fieldPath);
            Assert.thatUnchecked(resolved.size() <= 1, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
            if (resolved.size() == 1) {
                return resolved.get(0);
            }
        }
        Assert.failUnchecked(String.format("attempting to query non existing column '%s'", fieldPathStr), ErrorCode.INVALID_COLUMN_REFERENCE);
        return null;
    }

    @Nonnull
    private static List<FieldValue> resolveField(@Nonnull final Collection<Quantifier> quns,
                                                 @Nonnull final List<FieldValue.Accessor> fieldAccessors,
                                                 @Nonnull final List<String> fieldPath) {
        Assert.thatUnchecked(fieldPath.size() == fieldAccessors.size());
        final var result = ImmutableList.<FieldValue>builder();
        for (final var qun : quns) {
            if (fieldPath.size() > 1) { // maybe qualified, check if the first part matches the quantifier's alias itself.
                if (fieldPath.get(0).equals(qun.getAlias().getId()) && resolveFieldPath(qun.getFlowedObjectType(), fieldAccessors.subList(1, fieldAccessors.size()))) {
                    result.add(FieldValue.ofFieldNames(qun.getFlowedObjectValue(), fieldPath.subList(1, fieldPath.size())));
                }
            }  // not qualified, or nested field.
            if (resolveFieldPath(qun.getFlowedObjectType(), fieldAccessors)) {
                result.add(FieldValue.ofFieldNames(qun.getFlowedObjectValue(), fieldPath));
            }
        }
        return result.build();
    }

    @Nonnull
    private static Set<Quantifier> collectQuantifiersFromAncestorBlocks(@Nonnull final Scopes.Scope scope) {
        final var result = ImmutableSet.<Quantifier>builder();
        var ancestor = scope.getParent();
        while (ancestor != null) {
            result.addAll(ancestor.getForEachQuantifiers());
            ancestor = ancestor.getParent();
        }
        return result.build();
    }

    @Nonnull
    private static Pair<Type, Type> splitSelectWhereType(@Nonnull final Type type) {
        Assert.thatUnchecked(type.getTypeCode() == Type.TypeCode.RECORD);
        Type.Record record = (Type.Record) type;
        return Pair.of(Type.Record.fromFields(record.getFields().subList(0, 1)), Type.Record.fromFields(record.getFields().subList(1, record.getFields().size())));
    }

    @Nonnull
    private static Pair<List<Column<? extends FieldValue>>, List<Column<? extends FieldValue>>> splitSelectWhereFlownColumns(@Nonnull final Quantifier qun) {
        return Pair.of(qun.getFlowedColumns().subList(0, 1), qun.getFlowedColumns().subList(1, qun.getFlowedColumns().size()));
    }

    @Nonnull
    private static List<FieldValue.Accessor> toAccessors(@Nonnull final List<String> parts) {
        return parts.stream().map(fieldName -> new FieldValue.Accessor(fieldName, -1)).collect(ImmutableList.toImmutableList());
    }

    // this method is copied almost verbatim from RecLayer FieldValue.java
    private static boolean resolveFieldPath(@Nonnull final Type inputType, @Nonnull final List<FieldValue.Accessor> accessors) {
        final var accessorPathBuilder = ImmutableList.<Type.Record.Field>builder();
        var currentType = inputType;
        for (final var accessor : accessors) {
            final var fieldName = accessor.getFieldName();
            if (currentType.getTypeCode() != Type.TypeCode.RECORD) {
                return false;
            }
            final var recordType = (Type.Record) currentType;
            final var fieldNameFieldMap = Objects.requireNonNull(recordType.getFieldNameFieldMap());
            final Type.Record.Field field;
            if (fieldName != null) {
                if (!fieldNameFieldMap.containsKey(fieldName)) {
                    return false;
                }
                field = fieldNameFieldMap.get(fieldName);
            } else {
                // field is not accessed by field but by ordinal number
                Verify.verify(accessor.getOrdinalFieldNumber() >= 0);
                field = recordType.getFields().get(accessor.getOrdinalFieldNumber());
            }
            accessorPathBuilder.add(field);
            currentType = field.getFieldType();
        }
        return true;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, the analyzer " +
            "can not deduce that control flow leads to exception")
    @Nonnull
    public static RelationalExpression quantifyOver(@Nonnull final QualifiedIdentifierValue identifierValue,
                                                    @Nonnull final RelationalParserContext parserContext,
                                                    @Nonnull final AccessHints accessHintSet) {
        Assert.thatUnchecked(identifierValue.getParts().length <= 2);
        Assert.thatUnchecked(identifierValue.getParts().length > 0);

        if (!identifierValue.isQualified()) {
            final String recordTypeName = identifierValue.getParts()[0];
            Assert.notNullUnchecked(recordTypeName);
            final ImmutableSet<String> recordTypeNameSet = ImmutableSet.<String>builder().add(recordTypeName).build();
            final Map<String, Descriptors.FieldDescriptor> allAvailableRecordTypes = parserContext.getScannabledRecordTypes();
            final Set<String> allAvailableRecordTypeNames = parserContext.getScannableRecordTypeNames();
            final Optional<Type> recordType = parserContext.getTypeRepositoryBuilder().getTypeByName(recordTypeName);
            Assert.thatUnchecked(recordType.isPresent(), String.format("Unknown table %s", recordTypeName), ErrorCode.UNDEFINED_TABLE);
            Assert.thatUnchecked(allAvailableRecordTypeNames.contains(recordTypeName), String.format("attempt to scan non existing record type %s from record store containing (%s)",
                    recordTypeName, String.join(",", allAvailableRecordTypeNames)));
            return new LogicalTypeFilterExpression(recordTypeNameSet,
                    Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(allAvailableRecordTypeNames,
                            Type.Record.fromFieldDescriptorsMap(allAvailableRecordTypes),
                            accessHintSet))),
                    recordType.get());
        } else {
            final String qualifier = identifierValue.getParts()[0];
            Assert.notNullUnchecked(qualifier);
            final String recordTypeName = identifierValue.getParts()[1];
            Assert.notNullUnchecked(recordTypeName);
            // todo: check if the qualifier is actually a schema name
            final var maybeQun = parserContext.resolveQuantifier(qualifier, true /* PartiQL semantics */);
            Assert.thatUnchecked(maybeQun.isPresent(), String.format("attempt to query field %s from non-existing qualifier %s", recordTypeName, qualifier)); // TODO (yhatem) proper error code.
            final var qun = maybeQun.get();
            final QuantifiedValue value = qun.getFlowedObjectValue();
            Assert.thatUnchecked(value.getResultType().getTypeCode() == Type.TypeCode.RECORD, String.format("alias is not valid %s", qualifier));
            final Type.Record record = (Type.Record) value.getResultType();
            Assert.notNullUnchecked(record.getFieldNameFieldMap());
            Assert.thatUnchecked(Objects.requireNonNull(record.getFieldNameFieldMap()).containsKey(recordTypeName), String.format("field %s was not found in %s", recordTypeName, qualifier));
            final Type fieldType = Objects.requireNonNull(record.getFieldNameFieldMap()).get(recordTypeName).getFieldType();
            Assert.thatUnchecked(fieldType.getTypeCode() == Type.TypeCode.ARRAY, String.format("expecting an array-type field, however %s is not", recordTypeName));
            return new ExplodeExpression(FieldValue.ofFieldName(value, recordTypeName));
        }
    }

    @Nullable
    public static <T> T safeCastLiteral(@Nonnull final Object value, @Nonnull final Class<T> clazz) {
        Assert.thatUnchecked(value instanceof LiteralValue);
        final Object result = ((LiteralValue<?>) value).getLiteralValue();
        if (!clazz.isInstance(result)) {
            return null;
        }
        return clazz.cast(result);
    }

    @Nonnull
    public static Pair<Optional<URI>, String> parseSchemaIdentifier(@Nonnull final String id) {
        Assert.notNullUnchecked(id);
        if (id.startsWith("/")) {
            Assert.thatUnchecked(isProperDbUri(id), String.format("invalid database path '%s'", id), ErrorCode.INVALID_PATH);
            int separatorIdx = id.lastIndexOf("/");
            Assert.thatUnchecked(separatorIdx < id.length() - 1);
            return Pair.of(Optional.of(URI.create(id.substring(0, separatorIdx))), id.substring(separatorIdx + 1));
        } else {
            return Pair.of(Optional.empty(), id);
        }
    }

    @Nonnull
    public static Type.TypeCode toProtoType(@Nonnull final String text) {
        switch (text.toUpperCase(Locale.ROOT)) {
            case "STRING":
                return Type.TypeCode.STRING;
            case "INT64":
                return Type.TypeCode.LONG;
            case "DOUBLE":
                return Type.TypeCode.DOUBLE;
            case "BOOLEAN":
                return Type.TypeCode.BOOLEAN;
            case "BYTES":
                return Type.TypeCode.BYTES;
            default:
                return Type.TypeCode.RECORD;
        }
    }

    public static boolean isProperDbUri(@Nonnull final String path) {
        return normalizeString(path).matches("/\\w[a-zA-Z0-9_/]*\\w");
    }

    @Nonnull
    public static Column<Value> toColumn(@Nonnull final Value value) {
        if (value instanceof FieldValue) {
            return toColumn(value, ((FieldValue) value).getLastField().getFieldName());
        } else {
            return Column.unnamedOf(value);
        }
    }

    @Nonnull
    public static Column<Value> toColumn(@Nonnull final Value value, @Nonnull final String name) {
        return Column.of(Type.Record.Field.of(value.getResultType(), Optional.of(name)), value);
    }

    @Nonnull
    public static String toProtoBufCompliantName(@Nonnull final String input) {
        Assert.thatUnchecked(input.length() > 0);
        final var modified = input.replace("-", "_");
        final char c = input.charAt(0);
        if (c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
            return modified;
        }
        return "id" + modified;
    }

    public static void verifyAggregateValue(@Nonnull final AggregateValue value) {
        final var iterator = value.filter(c -> c instanceof AggregateValue).iterator();
        Assert.thatUnchecked(iterator.hasNext(), "internal error"); // since it must be `value` itself.
        iterator.next();
        Assert.thatUnchecked(!iterator.hasNext(),
                String.format("nested aggregate '%s' is not supported", value),
                ErrorCode.UNSUPPORTED_OPERATION);
    }

    public static boolean hasAggregation(@Nonnull final RelationalParser.SelectElementsContext selectElementsContext) {
        return selectElementsContext.selectElement().stream().anyMatch(element -> element.getChildCount() > 0 &&
                element.getChild(0) instanceof RelationalParser.AggregateFunctionCallContext);
    }

    public static int getLastFieldIndex(@Nonnull final Type type) {
        Assert.thatUnchecked(type instanceof Type.Record);
        final Type.Record record = (Type.Record) type;
        Assert.thatUnchecked(!record.getFields().isEmpty());
        return record.getFields().size() - 1;
    }
}
