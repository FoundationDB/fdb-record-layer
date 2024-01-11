/*
 * LiteralsUtils.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;

import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.CANNOT_CONVERT_TYPE;
import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;

public class LiteralsUtils {

    @Nonnull
    public static Value processLiteral(@Nullable final Object literal,
                                       @Nonnull final PlanGenerationContext context) {
        final var literalValue = new LiteralValue<>(literal);
        if (!context.shouldProcessLiteral()) {
            return literalValue;
        } else {
            final var literalIndex = context.addStrippedLiteral(literal);
            final var result = ConstantObjectValue.of(Quantifier.constant(), literalIndex, literalValue.getResultType());
            context.addLiteralReference(result);
            return result;
        }
    }

    @Nonnull
    public static ConstantObjectValue processComplexLiteral(int index,
                                                          Type type,
                                                          @Nonnull final PlanGenerationContext context) {
        final var result = ConstantObjectValue.of(Quantifier.constant(), index, type);
        if (context.shouldProcessLiteral()) {
            context.addLiteralReference(result);
        }
        return result;
    }

    public static ConstantObjectValue processPreparedStatementArrayParameter(@Nonnull final Array param,
                                                                             Type.Array type,
                                                                             @Nonnull final PlanGenerationContext context) {
        Type.Array resolvedType = type;
        final int index = context.startArrayLiteral();
        final var arrayElements = new ArrayList<>();
        try {
            if (type == null) {
                resolvedType = SqlTypeSupport.arrayMetadataToArrayType(((RowArray) param).getMetaData(), false);
            }
            try (ResultSet rs = param.getResultSet()) {
                while (rs.next()) {
                    arrayElements.add(rs.getObject(1));
                }
            }
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        if (!arrayElements.isEmpty()) {
            Assert.thatUnchecked(resolvedType.equals(resolveArrayTypeFromObjectsList(arrayElements)),
                    "Cannot convert literal to " + resolvedType, DATATYPE_MISMATCH);
        }
        for (final Object o : arrayElements) {
            processPreparedStatementParameter(o, resolvedType.getElementType(), context);
        }
        context.finishArrayLiteral();
        return LiteralsUtils.processComplexLiteral(index, resolvedType, context);
    }

    private static Type.Array resolveArrayTypeFromObjectsList(List<Object> objects) {
        return resolveArrayTypeFromElementTypes(
                objects.stream().map(o -> {
                    if (o instanceof byte[]) {
                        return Type.fromObject(ZeroCopyByteString.wrap((byte[]) o));
                    } else if (o instanceof Struct || o instanceof Array) {
                        throw new RelationalException("Array of complex types are not yet supported", CANNOT_CONVERT_TYPE)
                                .toUncheckedWrappedException();
                    } else {
                        return Type.fromObject(o);
                    }
                }).collect(Collectors.toList()));
    }

    public static Type.Array resolveArrayTypeFromValues(List<Value> values) {
        return resolveArrayTypeFromElementTypes(values.stream().map(Value::getResultType).collect(Collectors.toList()));
    }

    private static Type.Array resolveArrayTypeFromElementTypes(List<Type> types) {
        Type elementType;
        if (types.isEmpty()) {
            elementType = Type.nullType();
        } else {
            // all values must have the same type.
            final var distinctTypes = types.stream().filter(type -> type != Type.nullType()).distinct().collect(Collectors.toList());
            Assert.thatUnchecked(distinctTypes.size() == 1, "could not determine type of array literal", DATATYPE_MISMATCH);
            elementType = distinctTypes.get(0);
        }
        return new Type.Array(elementType);
    }

    public static Value processPreparedStatementStructParameter(@Nonnull final Struct param,
                                                               final Type.Record type,
                                                               @Nonnull final PlanGenerationContext context) {
        Type.Record resolvedType = type;
        final var index = context.startStructLiteral();
        Object[] attributes;
        try {
            if (type == null) {
                resolvedType = SqlTypeSupport.structMetadataToRecordType(((RelationalStruct) param).getMetaData(), false);
            }
            attributes = param.getAttributes();
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        Assert.thatUnchecked(resolvedType.getFields().size() == attributes.length);
        for (int i = 0; i < attributes.length; i++) {
            processPreparedStatementParameter(attributes[i], resolvedType.getFields().get(i).getFieldType(), context);
        }
        context.finishStructLiteral(resolvedType);
        return processComplexLiteral(index, resolvedType, context);
    }

    public static Value processPreparedStatementParameter(@Nonnull final Object param,
                                                          final Type type,
                                                          @Nonnull final PlanGenerationContext context) {
        if (param instanceof Array) {
            Assert.thatUnchecked(type == null || type.isArray(), "Array type field required as prepared statement parameter", DATATYPE_MISMATCH);
            return LiteralsUtils.processPreparedStatementArrayParameter((Array) param, (Type.Array) type, context);
        } else if (param instanceof Struct) {
            Assert.thatUnchecked(type == null || type.isRecord(), "Required type field required as prepared statement parameter", DATATYPE_MISMATCH);
            return LiteralsUtils.processPreparedStatementStructParameter((Struct) param, (Type.Record) type, context);
        } else if (param instanceof byte[]) {
            return LiteralsUtils.processLiteral(ZeroCopyByteString.wrap((byte[]) param), context);
        } else {
            return LiteralsUtils.processLiteral(param, context);
        }
    }

    @Nonnull
    public static QueryPredicate toQueryPredicate(@Nonnull final BooleanValue value,
                                                  @Nonnull CorrelationIdentifier innermostAlias,
                                                  @Nonnull final PlanGenerationContext context) {
        if (context.hasDdlAncestor()) {
            final var result = value.toQueryPredicate(ParserUtils.EMPTY_TYPE_REPOSITORY, innermostAlias);
            Assert.thatUnchecked(result.isPresent());
            return result.get();
        } else {
            final var result = value.toQueryPredicate(null, innermostAlias);
            Assert.thatUnchecked(result.isPresent());
            return result.get();
        }
    }
}
