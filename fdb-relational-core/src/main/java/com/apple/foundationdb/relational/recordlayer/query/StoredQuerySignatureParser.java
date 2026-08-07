/*
 * StoredQuerySignatureParser.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalParser;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parses a stored query's verbatim parameter signature text (e.g. {@code "(bigint > 5)"}) into positional declared
 * parameters. The signature is persisted as text and re-parsed here at warmup; each {@code ?} in the stored query body
 * maps positionally (1-based) to one signature entry.
 * <p>
 * Only primitive parameter types are supported for now; the declared range (if any) is parsed into a
 * {@link RangeConstraints} for numeric comparison and {@code BETWEEN} forms (used later for filtered-index selection).
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public final class StoredQuerySignatureParser {

    private StoredQuerySignatureParser() {
    }

    /**
     * Parses the signature text into a 1-based positional map of declared parameters.
     *
     * @param signatureText the verbatim signature text, or an empty string if the stored query declares no signature
     * @return a positional (1-based) map of declared parameters; empty if the signature text is empty
     */
    @Nonnull
    public static Map<Integer, PreparedParams.DeclaredParameter> parse(@Nonnull final String signatureText) {
        if (signatureText.isEmpty()) {
            return Map.of();
        }
        final var parameterList = QueryParser.parseStoredQueryParameterList(signatureText);
        final var declared = new LinkedHashMap<Integer, PreparedParams.DeclaredParameter>();
        final var parameters = parameterList.storedQueryParameter();
        for (int i = 0; i < parameters.size(); i++) {
            final var type = resolveType(parameters.get(i).parameterType);
            final var range = parseRange(parameters.get(i).parameterRange(), type);
            declared.put(i + 1, new PreparedParams.DeclaredParameter(type, range));
        }
        return declared;
    }

    /**
     * Builds a {@link RangeConstraints} from a parameter's declared range. Handles the comparison-list form
     * ({@code > 5}, {@code > 0 and < 100}) and {@code BETWEEN low AND high}; {@code IN} and non-numeric range constants
     * are not yet supported.
     *
     * @param ctx the range context, or {@code null} if the parameter declares no range
     * @param declaredType the parameter's declared (primitive) type; range constants are coerced to it
     * @return the range constraints, or {@code null} if no range was declared
     */
    @Nullable
    private static RangeConstraints parseRange(@Nullable final RelationalParser.ParameterRangeContext ctx,
                                               @Nonnull final Type declaredType) {
        if (ctx == null) {
            return null;
        }
        final var builder = RangeConstraints.newBuilder();
        if (ctx.BETWEEN() != null) {
            // BETWEEN low AND high  ==>  >= low AND <= high
            builder.addComparisonMaybe(new Comparisons.SimpleComparison(
                    Comparisons.Type.GREATER_THAN_OR_EQUALS, numericComparand(ctx.low, declaredType)));
            builder.addComparisonMaybe(new Comparisons.SimpleComparison(
                    Comparisons.Type.LESS_THAN_OR_EQUALS, numericComparand(ctx.high, declaredType)));
        } else if (ctx.IN() != null) {
            throw new RelationalException("IN ranges are not yet supported in stored query signatures",
                    ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
        } else {
            final var operators = ctx.comparisonOperator();
            final var constants = ctx.constant();
            for (int i = 0; i < operators.size(); i++) {
                builder.addComparisonMaybe(new Comparisons.SimpleComparison(
                        comparisonType(operators.get(i)), numericComparand(constants.get(i), declaredType)));
            }
        }
        return builder.build().orElse(null);
    }

    @Nonnull
    private static Comparisons.Type comparisonType(@Nonnull final RelationalParser.ComparisonOperatorContext ctx) {
        switch (ctx.getText()) {
            case "=":
                return Comparisons.Type.EQUALS;
            case ">":
                return Comparisons.Type.GREATER_THAN;
            case "<":
                return Comparisons.Type.LESS_THAN;
            case ">=":
                return Comparisons.Type.GREATER_THAN_OR_EQUALS;
            case "<=":
                return Comparisons.Type.LESS_THAN_OR_EQUALS;
            default:
                throw new RelationalException("unsupported comparison operator in stored query signature range",
                        ErrorCode.UNSUPPORTED_OPERATION).addContext("operator", ctx.getText()).toUncheckedWrappedException();
        }
    }

    /**
     * Extracts a numeric range-bound constant and coerces it to the declared type's Java representation, so the range
     * comparand type matches the runtime value/index-predicate comparand. Only numeric constants are supported for now.
     */
    @Nonnull
    private static Object numericComparand(@Nonnull final RelationalParser.ConstantContext ctx,
                                           @Nonnull final Type declaredType) {
        final Object raw;
        if (ctx instanceof RelationalParser.DecimalConstantContext) {
            raw = ParseHelpers.parseDecimal(((RelationalParser.DecimalConstantContext) ctx).decimalLiteral().getText());
        } else if (ctx instanceof RelationalParser.NegativeDecimalConstantContext) {
            final var magnitude = (Number) ParseHelpers.parseDecimal(
                    ((RelationalParser.NegativeDecimalConstantContext) ctx).decimalLiteral().getText());
            raw = negate(magnitude);
        } else {
            throw new RelationalException("only numeric range bounds are supported in stored query signatures",
                    ErrorCode.UNSUPPORTED_OPERATION).addContext("constant", ctx.getText()).toUncheckedWrappedException();
        }
        return coerce((Number) raw, declaredType);
    }

    @Nonnull
    private static Number negate(@Nonnull final Number value) {
        if (value instanceof Long) {
            return -value.longValue();
        }
        if (value instanceof Integer) {
            return -value.intValue();
        }
        return -value.doubleValue();
    }

    @Nonnull
    private static Object coerce(@Nonnull final Number value, @Nonnull final Type declaredType) {
        switch (declaredType.getTypeCode()) {
            case LONG:
                return value.longValue();
            case INT:
                return value.intValue();
            case DOUBLE:
                return value.doubleValue();
            case FLOAT:
                return value.floatValue();
            default:
                throw new RelationalException("range bounds are only supported for numeric declared types",
                        ErrorCode.UNSUPPORTED_OPERATION).addContext("typeCode", declaredType.getTypeCode())
                        .toUncheckedWrappedException();
        }
    }

    @Nonnull
    private static Type resolveType(@Nonnull final RelationalParser.FunctionColumnTypeContext ctx) {
        if (ctx.primitiveType() == null || ctx.ARRAY() != null) {
            throw new RelationalException("only primitive-typed stored query parameters are supported",
                    ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
        }
        final Type.TypeCode code;
        switch (ctx.primitiveType().getText().toUpperCase(Locale.ROOT)) {
            case "STRING":
                code = Type.TypeCode.STRING;
                break;
            case "INTEGER":
                code = Type.TypeCode.INT;
                break;
            case "BIGINT":
                code = Type.TypeCode.LONG;
                break;
            case "DOUBLE":
                code = Type.TypeCode.DOUBLE;
                break;
            case "FLOAT":
                code = Type.TypeCode.FLOAT;
                break;
            case "BOOLEAN":
                code = Type.TypeCode.BOOLEAN;
                break;
            case "BYTES":
                code = Type.TypeCode.BYTES;
                break;
            default:
                throw new RelationalException("unsupported stored query parameter type",
                        ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
        }
        return Type.primitiveType(code, true);
    }
}
