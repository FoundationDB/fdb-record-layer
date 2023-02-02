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
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * Contains a set of utility methods that are relevant for parsing the AST.
 */
public final class ParserUtils {

    private static final Map<String, BuiltInFunction<? extends Value>> functionMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // used only to be passed to expression lambdas in Record Layer (to be removed).
    @Nonnull
    private static final TypeRepository.Builder emptyBuilder = TypeRepository.newBuilder();

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
        functionMap.put("IN", new InOpValue.InFn());

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
        functionMap.put("MAX_EVER", new IndexOnlyAggregateValue.MaxEverLongFn());
        functionMap.put("MIN_EVER", new IndexOnlyAggregateValue.MinEverLongFn());
        functionMap.put("SUM", new NumericAggregationValue.SumFn());
        functionMap.put("COUNT", new CountValue.CountFn());
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

    public static int parseBoundInteger(@Nonnull final String valueAsString,
                                        @Nullable Integer lowerbound, @Nullable Integer upperbound) {
        int value = Integer.parseInt(valueAsString);
        if (lowerbound != null) {
            Assert.thatUnchecked(value >= lowerbound, String.format("Parsed Integer cannot be less than %d", lowerbound));
        }
        if (upperbound != null) {
            Assert.thatUnchecked(value <= upperbound, String.format("Parsed Integer cannot be greater than %d", upperbound));
        }
        return value;
    }

    public static FieldValue resolveField(@Nonnull final List<String> fieldPath, @Nonnull final Scopes scopes) {
        final var currentScope = scopes.getCurrentScope();
        Assert.thatUnchecked(!fieldPath.isEmpty());
        final var isUnderlyingSelectWhere = currentScope.isFlagSet(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE);
        if (isUnderlyingSelectWhere) {
            return resolveFieldGroupedQuantifier(fieldPath, scopes);
        } else {
            return resolveFieldSimpleQuantifier(fieldPath, scopes);
        }
    }

    @Nonnull
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    private static FieldValue resolveFieldGroupedQuantifier(@Nonnull final List<String> fieldPath, @Nonnull final Scopes scopes) {
        final var currentScope = scopes.getCurrentScope();
        Assert.thatUnchecked(!fieldPath.isEmpty());
        FieldValue result = null;
        var fieldAccessors = toAccessors(fieldPath);
        final var isResolvingAggregations = currentScope.isFlagSet(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
        final var isResolvingSelectHaving = currentScope.isFlagSet(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING);
        final var fieldPathStr = String.join(".", fieldPath);

        // Try to resolve the field by looking into the visible part of the only quantifier we have.
        Assert.thatUnchecked(currentScope.getForEachQuantifiers().size() == 1);
        final var qun = currentScope.getForEachQuantifiers().get(0);
        final var types = splitSelectWhereType(qun.getFlowedObjectType());

        final var visibleType = isResolvingAggregations ? types.getRight() : types.getLeft();
        if (resolveFieldPath(visibleType, fieldAccessors)) {
            result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), fieldPath);
        }

        final var flownColumnsSplit = splitSelectWhereFlownColumns(qun);
        final var visibleColumns = isResolvingAggregations ? flownColumnsSplit.getRight() : flownColumnsSplit.getLeft();
        final var visibleColumnsDeref = visibleColumns.stream().map(c -> Column.of(c.getField(), dereference(c.getValue(), scopes))).collect(toList());
        for (final var visibleField : visibleColumnsDeref) {
            final var columnPathWithQuantifier = isResolvingAggregations ?
                    ImmutableList.<String>builder().add(visibleField.getField().getFieldName()).addAll(fieldPath).build() :
                    ImmutableList.<String>builder().add(visibleField.getField().getFieldName()).add(fieldPath.get(fieldPath.size() - 1)).build();
            final var resolved = resolveFieldPath(qun.getFlowedObjectValue().getResultType(), toAccessors(columnPathWithQuantifier));
            if (resolved) {
                Assert.isNullUnchecked(result, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
                result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), columnPathWithQuantifier);
            }
        }

        if (result != null) {
            return result;
        }

        // corner case:
        if (!isResolvingAggregations && fieldPath.size() > 1) {
            for (final var groupingColumnField : visibleColumnsDeref.stream().map(Column::getField).collect(toList())) {
                if ((isResolvingSelectHaving || resolveFieldPath(types.getRight(), fieldAccessors)) && resolveFieldPath(groupingColumnField.getFieldType(), fieldAccessors.subList(1, fieldAccessors.size()))) {
                    Assert.isNullUnchecked(result, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
                    result = FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.<String>builder().add(visibleColumnsDeref.get(0).getField().getFieldName()).addAll(fieldPath.subList(1, fieldPath.size())).build());
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
            final var ancestorQuns = collectQuantifiersFromAncestorBlocks(currentScope);
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
    private static FieldValue resolveFieldSimpleQuantifier(@Nonnull final List<String> fieldPath, @Nonnull final Scopes scopes) {
        final var currentScope = scopes.getCurrentScope();
        final var fieldAccessors = toAccessors(fieldPath);
        final var fieldPathStr = String.join(".", fieldPath);

        // Try to resolve the field by looking into all quantifiers in current scope, if exactly one field is found, return it.
        // precedence resolves potential ambiguity with similarly named fields in top levels, i.e. do not look for ambiguity issues
        // if we already find a matching field in current scope.
        {
            final var resolved = resolveField(currentScope.getForEachQuantifiers(), fieldAccessors, fieldPath);
            Assert.thatUnchecked(resolved.size() <= 1, String.format("ambiguous column name '%s'", fieldPathStr), ErrorCode.AMBIGUOUS_COLUMN);
            if (resolved.size() == 1) {
                return resolved.get(0);
            }
        }

        // We have not found the field in current scope, look into parent scopes for potential matches
        {
            final var ancestorQuns = collectQuantifiersFromAncestorBlocks(currentScope);
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
            final var fieldName = accessor.getName();
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
                Verify.verify(accessor.getOrdinal() >= 0);
                field = recordType.getFields().get(accessor.getOrdinal());
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
                                                    @Nonnull final PlanGenerationContext context,
                                                    @Nonnull final Scopes scopes,
                                                    @Nonnull final AccessHints accessHintSet) {
        Assert.thatUnchecked(identifierValue.getParts().length <= 2);
        Assert.thatUnchecked(identifierValue.getParts().length > 0);

        if (!identifierValue.isQualified()) {
            final String recordTypeName = identifierValue.getParts()[0];
            Assert.notNullUnchecked(recordTypeName);
            final ImmutableSet<String> recordTypeNameSet = ImmutableSet.<String>builder().add(recordTypeName).build();
            final var allAvailableRecordTypes = meldTableTypes(context.asDql().getRecordLayerSchemaTemplate());
            final Set<String> allAvailableRecordTypeNames = context.asDql().getScannableRecordTypeNames();
            final Optional<Type> recordType = context.asDql().getRecordLayerSchemaTemplate().findTableByName(recordTypeName).map(t -> ((RecordLayerTable) t).getType());
            Assert.thatUnchecked(recordType.isPresent(), String.format("Unknown table %s", recordTypeName), ErrorCode.UNDEFINED_TABLE);
            Assert.thatUnchecked(allAvailableRecordTypeNames.contains(recordTypeName), String.format("attempt to scan non existing record type %s from record store containing (%s)",
                    recordTypeName, String.join(",", allAvailableRecordTypeNames)));
            // we explicitly do not add this quantifier to the scope, so it doesn't cause name resolution errors due to duplicate identifiers.
            return new LogicalTypeFilterExpression(recordTypeNameSet,
                    Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(allAvailableRecordTypeNames, allAvailableRecordTypes, accessHintSet))),
                    recordType.get());
        } else {
            final String qualifier = identifierValue.getParts()[0];
            Assert.notNullUnchecked(qualifier);
            final String recordTypeName = identifierValue.getParts()[1];
            Assert.notNullUnchecked(recordTypeName);
            // todo: check if the qualifier is actually a schema name
            final var maybeQun = scopes.resolveQuantifier(qualifier, true /* PartiQL semantics */);
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
    public static Type.TypeCode toRecordLayerType(@Nonnull final String text) {
        switch (text.toUpperCase(Locale.ROOT)) {
            case "STRING":
                return Type.TypeCode.STRING;
            case "INT32":
                return Type.TypeCode.INT;
            case "INT64":
                return Type.TypeCode.LONG;
            case "DOUBLE":
                return Type.TypeCode.DOUBLE;
            case "BOOLEAN":
                return Type.TypeCode.BOOLEAN;
            case "BYTES":
                return Type.TypeCode.BYTES;
            default: // assume it is a custom type, will fail in upper layers if the type can not be resolved.
                return Type.TypeCode.RECORD;
        }
    }

    @Nonnull
    public static DataType toRelationalType(@Nonnull final String typeString,
                                          boolean isNullable,
                                          boolean isRepeated,
                                          @Nonnull final RecordLayerSchemaTemplate.Builder metadataBuilder) {
        DataType type = null;
        switch (typeString.toUpperCase(Locale.ROOT)) {
            case "STRING":
                type = isNullable ? DataType.Primitives.NULLABLE_STRING.type() : DataType.Primitives.STRING.type();
                break;
            case "INT32":
                type = isNullable ? DataType.Primitives.NULLABLE_INTEGER.type() : DataType.Primitives.INTEGER.type();
                break;
            case "INT64":
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
            default: // assume it is a custom type, will fail in upper layers if the type can not be resolved.
                // lookup the type (Struct, Table, or Enum) in the schema template metadata under construction.
                final var maybeFound = metadataBuilder.findType(typeString);
                // if we can not find the type now, mark it, we will try to resolve it later on via a second pass.
                type = maybeFound.orElseGet(() -> DataType.UnknownType.of(typeString, isNullable));
                break;
        }

        if (isRepeated) {
            return DataType.ArrayType.from(type, isNullable);
        } else {
            return type;
        }

    }

    public static boolean isProperDbUri(@Nonnull final String path) {
        return normalizeString(path).matches("/\\w[a-zA-Z0-9_/]*\\w");
    }

    @Nonnull
    public static Column<Value> toColumn(@Nonnull final Value value) {
        if (value instanceof FieldValue) {
            return toColumn(value, ((FieldValue) value).getLastFieldName().orElseThrow());
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
        final var iterator = value.filter(c -> c instanceof StreamableAggregateValue || c instanceof IndexableAggregateValue).iterator();
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

    @Nonnull
    public static String toString(@Nonnull final Value value) {
        if (value instanceof LiteralValue) {
            return Objects.requireNonNull(((LiteralValue<?>) value).getLiteralValue()).toString();
        } else {
            return value.toString();
        }
    }

    @Nonnull
    public static String underlineParsingError(@Nonnull final Recognizer<?, ?> recognizer,
                                               @Nonnull Token offendingToken,
                                               int line,
                                               int charPositionInLine) {
        // I got this recipe from the book: "The Definitive ANTLR 4 Reference, 2nd Edition".
        final StringBuilder stringBuilder = new StringBuilder();
        final CommonTokenStream tokens = (CommonTokenStream) recognizer.getInputStream();
        final String input = tokens.getTokenSource().getInputStream().toString();
        final String[] lines = input.split("\n");
        final String errorLine = lines[line - 1];
        stringBuilder.append(errorLine).append("\n");
        stringBuilder.append(" ".repeat(Math.max(0, charPositionInLine)));
        int start = offendingToken.getStartIndex();
        int stop = offendingToken.getStopIndex();
        if (stop < start) {
            stringBuilder.append("^^"); // missing token
        } else if (start >= 0) {
            stringBuilder.append("^".repeat(Math.max(0, stop - start + 1)));
        }
        return stringBuilder.toString();
    }

    @Nonnull
    public static <T extends Typed> Typed encapsulate(BuiltInFunction<T> function, @Nonnull final List<? extends Typed> arguments) {
        return function.encapsulate(emptyBuilder, arguments.stream().map(ParserUtils::flattenRecordWithOneField).collect(toList()));
    }

    @Nonnull
    private static Typed flattenRecordWithOneField(@Nonnull final Typed value) {
        if (value instanceof RecordConstructorValue && ((RecordConstructorValue) value).getColumns().size() == 1) {
            return flattenRecordWithOneField(((Value) value).getChildren().iterator().next());
        }
        if (value instanceof Value) {
            return ((Value) value).withChildren(StreamSupport.stream(((Value) value).getChildren().spliterator(), false).map(ParserUtils::flattenRecordWithOneField).map(v -> (Value) v).collect(toList()));
        }
        return value;
    }

    // TODO (yhatem) get rid of this method once we change the way we model having multiple record types in scan operator.
    @Nonnull
    public static Type.Record meldTableTypes(@Nonnull final RecordLayerSchemaTemplate schemaTemplate) {
        // todo (yhatem): should be removed once this is addressed https://github.com/FoundationDB/fdb-record-layer/issues/1884
        final var meldedFields = schemaTemplate.getTables()
                .stream()
                .sorted(Comparator.comparing(RecordLayerTable::getName))
                .filter(Objects::nonNull)
                .flatMap(table -> table.getType().getFields().stream())
                .collect(
                        Collectors.groupingBy(
                                Type.Record.Field::getFieldName,
                                LinkedHashMap::new,
                                Collectors.reducing(
                                        null,
                                        (field1, field2) -> {
                                            if (field1 == null) {
                                                return field2;
                                            }
                                            if (field2 == null) {
                                                return field1;
                                            }
                                            if (field1.getFieldType().getJavaClass().equals(field2.getFieldType().getJavaClass())) {
                                                return field1;
                                            }
                                            throw new IllegalArgumentException("cannot form union type of two fields sharing the same name with different types");
                                        })));
        return Type.Record.fromFields(new ArrayList<>(meldedFields.values()));
    }

    @Nonnull
    public static List<? extends Typed> validateInValuesList(List<? extends Value> values) {
        final Set<Value> valueSet = new HashSet<>();
        final List<Value> toReturn = new ArrayList<>();
        for (final Value value : values) {
            if (value.getResultType() == Type.NULL) {
                Assert.failUnchecked("NULL values are not allowed in the IN list", ErrorCode.WRONG_OBJECT_TYPE);
            }
            if (value.getResultType().isUnresolved()) {
                Assert.failUnchecked(String.format("Type cannot be determined for element `%s` in the IN list", value),
                        ErrorCode.UNKNOWN_TYPE);
            }
            // compile-time de-duplication
            if (valueSet.contains(value)) {
                continue;
            }
            toReturn.add(value);
            valueSet.add(value);
        }
        return toReturn;
    }

    @Nonnull
    public static Value resolveDefaultValue(@Nonnull final Type type) {
        // TODO use metadata default values -- for now just do this:
        //
        // resolution rules:
        // - type is nullable ==> null
        // - type is not nullable
        //   - type is of an array type ==> empty array
        //   - type is a numeric type ==> 0 element
        //   - type is string ==> ''
        //   - type is a boolean ==> false
        //   - type is bytes ==> zero length byte array
        //   - type is record or enum ==> error
        if (type.isNullable()) {
            return new NullValue(type);
        } else {
            switch (type.getTypeCode()) {
                case UNKNOWN:
                case ANY:
                case NULL:
                    throw Assert.failUnchecked("internal typing error; target type is not properly resolved");
                case BOOLEAN:
                    return LiteralValue.ofScalar(false);
                case BYTES:
                    return LiteralValue.ofScalar(ByteString.empty());
                case DOUBLE:
                    return LiteralValue.ofScalar(0.0d);
                case FLOAT:
                    return LiteralValue.ofScalar(0.0f);
                case INT:
                    return LiteralValue.ofScalar(0);
                case LONG:
                    return LiteralValue.ofScalar(0L);
                case STRING:
                    return LiteralValue.ofScalar("");
                case ENUM:
                    throw Assert.failUnchecked("non-nullable enums must be specified", ErrorCode.CANNOT_CONVERT_TYPE);
                case RECORD:
                    throw Assert.failUnchecked("non-nullable records must be specified", ErrorCode.CANNOT_CONVERT_TYPE);
                case ARRAY:
                    final var elementType = Assert.notNullUnchecked(((Type.Array) type).getElementType());
                    return AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArray(elementType);
                default:
                    throw Assert.failUnchecked("unsupported type");
            }
        }
    }

    // warning: expensive, TODO (Relational semantic analysis)
    @Nonnull
    public static Value dereference(@Nonnull final Value value,
                                    @Nonnull final Scopes scopes) {
        if (value instanceof RecordConstructorValue) {
            return RecordConstructorValue.ofColumns(
                    ((RecordConstructorValue) value).getColumns()
                            .stream()
                            .map(c -> Column.of(c.getField(), dereference(c.getValue(), scopes)))
                            .collect(toList()));
        } else if (value instanceof CountValue) {
            final var children = StreamSupport.stream(value.getChildren().spliterator(), false).collect(toList());
            Verify.verify(children.size() <= 1);
            if (!children.isEmpty()) {
                return value.withChildren(Collections.singleton(dereference(children.get(0), scopes)));
            } else {
                return value;
            }
        } else if (value instanceof FieldValue || value instanceof IndexableAggregateValue || value instanceof NumericAggregationValue) {
            final var valueWithChild = (ValueWithChild) value;
            return valueWithChild.withNewChild(dereference(valueWithChild.getChild(), scopes));
        } else if (value instanceof QuantifiedObjectValue) {
            final var qov = (QuantifiedObjectValue) value;
            final var quantifier = scopes.resolveQuantifier(qov.getAlias(), true);
            return quantifier.map(q -> dereference(q.getRangesOver().get().getResultValue(), scopes)).orElse(value);
        } else {
            return value;
        }
    }
}
