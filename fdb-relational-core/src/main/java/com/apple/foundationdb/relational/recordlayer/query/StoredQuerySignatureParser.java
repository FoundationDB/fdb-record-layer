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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalParser;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parses a stored query's verbatim parameter signature text (e.g. {@code "(bigint > 5)"}) into positional declared
 * parameters. The signature is persisted as text and re-parsed here at warmup; each {@code ?} in the stored query body
 * maps positionally (1-based) to one signature entry.
 * <p>
 * Only primitive parameter types are supported for now; the declared range (if any) is not yet extracted.
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
            // TODO: extract the declared range from parameters.get(i).parameterRange() for filtered-index selection.
            declared.put(i + 1, new PreparedParams.DeclaredParameter(type, null));
        }
        return declared;
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
