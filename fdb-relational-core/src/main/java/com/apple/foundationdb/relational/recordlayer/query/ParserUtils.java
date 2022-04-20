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

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Scopes;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.apple.foundationdb.relational.recordlayer.query.AstVisitor.UNSUPPORTED_QUERY;

/**
 * Contains a set of utility methods that are relevant for parsing the AST.
 */
public final class ParserUtils {

    private static final Map<String, BuiltInFunction<Value>> infixToFunction = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private ParserUtils() {
    }

    /**
     * If a string is single- or double-quoted, removes the quotation, otherwise, returns the string as-is.
     * @param quotedString The quoted string.
     * @return same string without quotes (if any).
     */
    @Nullable
    public static String unquoteString(@Nullable final String quotedString) {
        if (quotedString == null) {
            return null;
        }
        if (isQuoted(quotedString, "'") || isQuoted(quotedString, "\"")) {
            return quotedString.substring(1, quotedString.length() - 1);
        }
        return quotedString;
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

    @Nullable
    public static FieldValue getFieldValue(@Nonnull final String fieldName, @Nonnull final Value identifier) {
        Assert.thatUnchecked(identifier.getResultType() instanceof Type.Record);
        final Type.Record recordType = (Type.Record) identifier.getResultType();
        final Map<String, Type> fieldTypeMap = Objects.requireNonNull(recordType.getFieldTypeMap());
        Assert.thatUnchecked(fieldTypeMap.containsKey(fieldName), "attempting to query non existing field " + fieldName);
        final Type fieldType = fieldTypeMap.get(fieldName);
        if (identifier instanceof FieldValue) {
            ImmutableList.Builder<String> fieldPathBuilder = ImmutableList.builder();
            fieldPathBuilder.addAll(((FieldValue) identifier).getFieldPath());
            fieldPathBuilder.add(fieldName);
            return new FieldValue(((FieldValue) identifier).getChild(), fieldPathBuilder.build(), fieldType);
        } else if (identifier instanceof QuantifiedObjectValue) {
            return new FieldValue((QuantifiedObjectValue) identifier, ImmutableList.of(fieldName), fieldType);
        }
        return null;
    }

    @Nullable
    public static FieldValue getFieldValue(@Nonnull final String fieldName, @Nonnull final ParserContext parserContext) {
        Scopes.Scope scope = parserContext.getCurrentScope();
        FieldValue result = null;
        boolean matchFound = false;
        while (scope != null)  {
            final Map<CorrelationIdentifier, QuantifiedValue> boundIdentifiers = scope.getBoundQuantifiers();
            for (final Value value : boundIdentifiers.values()) {
                if (value.getResultType() instanceof Type.Record) {
                    final Type.Record recordType = (Type.Record) value.getResultType();
                    final Map<String, Type> fieldTypeMap = Objects.requireNonNull(recordType.getFieldTypeMap());
                    if (fieldTypeMap.containsKey(fieldName)) {
                        if (matchFound) {
                            Assert.failUnchecked(UNSUPPORTED_QUERY); // ambiguous field name
                        } else {
                            matchFound = true;
                        }
                        final Type fieldType = fieldTypeMap.get(fieldName);
                        if (value instanceof FieldValue) {
                            ImmutableList.Builder<String> fieldPathBuilder = ImmutableList.builder();
                            fieldPathBuilder.addAll(((FieldValue) value).getFieldPath());
                            fieldPathBuilder.add(fieldName);
                            result = new FieldValue(((FieldValue) value).getChild(), fieldPathBuilder.build(), fieldType);
                        } else if (value instanceof QuantifiedObjectValue) {
                            result = new FieldValue((QuantifiedObjectValue) value, ImmutableList.of(fieldName), fieldType);
                        }
                    }
                }
            }
            scope = scope.getParentScope();
        }
        return result;
    }

    @Nullable
    public static <T> T safeCast(@Nonnull final Typed value, @Nonnull final Class<T> clazz) {
        Assert.thatUnchecked(value instanceof LiteralValue);
        final Object result = ((LiteralValue<?>) value).getLiteralValue();
        if (!clazz.isInstance(result)) {
            return null;
        }
        return clazz.cast(result);
    }
}
