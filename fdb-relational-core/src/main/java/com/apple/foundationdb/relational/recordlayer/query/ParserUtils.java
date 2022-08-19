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
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Scopes;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.tuple.Pair;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

    private static final Map<String, BuiltInFunction<Value>> infixToFunction = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

    @Nonnull
    public static String trimStartingDot(@Nonnull final String in) {
        Assert.thatUnchecked(in.length() > 0);
        Assert.thatUnchecked(in.charAt(0) == '.');
        return in.substring(1);
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
        infixToFunction.put("=", new RelOpValue.EqualsFn());
        infixToFunction.put(">", new RelOpValue.GtFn());
        infixToFunction.put("<", new RelOpValue.LtFn());
        infixToFunction.put("<=", new RelOpValue.LteFn());
        infixToFunction.put(">=", new RelOpValue.GteFn());
        infixToFunction.put("<>", new RelOpValue.NotEqualsFn());
        infixToFunction.put("!=", new RelOpValue.NotEqualsFn());

        infixToFunction.put("*", new ArithmeticValue.MulFn());
        infixToFunction.put("/", new ArithmeticValue.DivFn());
        infixToFunction.put("DIV", new ArithmeticValue.DivFn()); // should be case-insensitive
        infixToFunction.put("%", new ArithmeticValue.ModFn());
        infixToFunction.put("MOD", new ArithmeticValue.ModFn()); // should be case-insensitive
        infixToFunction.put("+", new ArithmeticValue.AddFn());
        infixToFunction.put("-", new ArithmeticValue.SubFn());
    }

    @Nonnull
    public static BuiltInFunction<Value> getFunction(@Nonnull final String infixNotation) {
        BuiltInFunction<Value> result = infixToFunction.get(infixNotation);
        Assert.notNullUnchecked(result, String.format("unsupported function '%s'", infixNotation));
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

    @Nonnull
    public static FieldValue getFieldValue(@Nonnull final List<String> fieldName, @Nonnull final Value identifier) {
        Assert.thatUnchecked(identifier.getResultType() instanceof Type.Record);
        try {
            return new FieldValue(identifier, fieldName);
        } catch (SemanticException se) {
            if (se.getMessage().contains("record does not contain specified field")) { // todo exchange error codes
                Assert.failUnchecked(String.format("attempting to query non existing field '%s'", fieldName.get(fieldName.size() - 1)));
            } else {
                Assert.failUnchecked(se.getMessage());
            }
        }
        assert false; // unreachable, but we should make the compiler happy.
        return null;
    }

    @Nonnull
    public static Quantifier findFieldPath(@Nonnull final String topLevelFieldPart, @Nonnull final ParserContext parserContext) {
        Quantifier result = null;
        boolean matchFound = false;
        final var quantifier = parserContext.getCurrentScope().getQuantifier(topLevelFieldPart);
        if (quantifier.isPresent()) {
            result = quantifier.get();
            matchFound = true;
        }
        Scopes.Scope scope = parserContext.getCurrentScope();
        for (final var qun : scope.getAllQuantifiers().stream().filter(q -> q instanceof Quantifier.ForEach).collect(Collectors.toList())) {
            for (final Column<? extends Value> column : qun.getFlowedColumns()) {
                if (column.getField().getFieldName().equals(topLevelFieldPart)) {
                    if (matchFound) {
                        Assert.failUnchecked(String.format("ambiguous column name '%s'", topLevelFieldPart), ErrorCode.AMBIGUOUS_COLUMN);
                    } else {
                        matchFound = true;
                        result = qun;
                    }
                }
            }
        }
        Assert.notNullUnchecked(result, String.format("could not find field name '%s'", topLevelFieldPart));
        return result;
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
            parserContext.addFilteredRecord(recordTypeName);
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
            final var maybeQun = parserContext.resolveQuantifier(qualifier);
            Assert.thatUnchecked(maybeQun.isPresent(), String.format("attempt to field %s from non-existing qualifier %s", recordTypeName, qualifier));
            final var qun = maybeQun.get();
            final QuantifiedValue value = qun.getFlowedObjectValue();
            Assert.thatUnchecked(value.getResultType().getTypeCode() == Type.TypeCode.RECORD, String.format("alias is not valid %s", qualifier));
            final Type.Record record = (Type.Record) value.getResultType();
            Assert.thatUnchecked(record.getFieldTypeMap().containsKey(recordTypeName), String.format("field %s was not found in %s", recordTypeName, qualifier));
            final Type fieldType = record.getFieldTypeMap().get(recordTypeName);
            Assert.thatUnchecked(fieldType.getTypeCode() == Type.TypeCode.ARRAY, String.format("expecting an array-type field, however %s is not", recordTypeName));
            return new ExplodeExpression(new FieldValue(value, ImmutableList.of(recordTypeName)));
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

    public static boolean requiresCanonicalSubSelect(@Nonnull final Quantifier quantifier, @Nonnull final ParserContext parserContext) {
        final var correlatesTo = quantifier.getCorrelatedTo();
        if (correlatesTo.isEmpty()) {
            return false; // all good.
        }
        // if one of the correlations is NOT found in parents, it means the correlation _might_ belong to a same-level subquery,
        // in that case, push it down via canonical select to be sure we do not have same-level correlations.
        return correlatesTo.stream().anyMatch(correlation -> parserContext.getCurrentScope().getQuantifier(correlation).isPresent());
    }
}
