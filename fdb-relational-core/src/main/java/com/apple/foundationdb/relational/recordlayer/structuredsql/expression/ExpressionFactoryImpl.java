/*
 * ExpressionFactoryImpl.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql.expression;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFactory;
import com.apple.foundationdb.relational.api.fluentsql.expression.Field;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.query.ParserUtils;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Immutable
public class ExpressionFactoryImpl implements ExpressionFactory {

    @Nonnull
    private final SchemaTemplate schemaTemplate;

    @Nonnull
    private final Options options;

    public ExpressionFactoryImpl(@Nonnull final SchemaTemplate schemaTemplate,
                                 @Nonnull final Options options) {
        this.schemaTemplate = schemaTemplate;
        this.options = options;
    }

    @Nonnull
    Field<?> resolve(@Nonnull final DataType type, @Nonnull final Iterable<String> fieldParts) {
        final boolean caseSensitive = options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
        DataType current = type;
        final ImmutableList.Builder<String> normalizedFieldParts = ImmutableList.builder();
        for (final var fieldPart : fieldParts) {
            Assert.thatUnchecked(
                    current instanceof DataType.StructType,
                    ErrorCode.INVALID_COLUMN_REFERENCE,
                    "invalid field reference %s",
                    fieldParts);
            final var normalizedFieldPart = Assert.notNullUnchecked(ParserUtils.normalizeString(fieldPart, caseSensitive));
            normalizedFieldParts.add(normalizedFieldPart);
            final var result =
                    ((DataType.StructType) current)
                            .getFields().stream()
                            .filter(f -> Assert.notNullUnchecked(f.getName()).equals(normalizedFieldPart))
                            .findAny();
            Assert.thatUnchecked(
                    result.isPresent(),
                    ErrorCode.INVALID_COLUMN_REFERENCE,
                    "invalid field reference '%s'",
                    StreamSupport.stream(fieldParts.spliterator(), false).collect(Collectors.joining(".")));
            current = result.get().getType();
        }
        return new FieldImpl<>(normalizedFieldParts.build(), this, current);
    }

    @Nonnull
    @Override
    public Field<?> field(@Nonnull final String tableName, @Nonnull Iterable<String> parts) {
        final boolean caseSensitive = options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
        final var normalizedName = Assert.notNullUnchecked(ParserUtils.normalizeString(tableName, caseSensitive));
        final Optional<Table> maybeTable;
        try {
            maybeTable = schemaTemplate.findTableByName(normalizedName); // could be a performance hit.
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
        Assert.thatUnchecked(maybeTable.isPresent(), String.format("Could not find table '%s'", normalizedName));
        return resolve(maybeTable.get().getDatatype(), parts);
    }
}
