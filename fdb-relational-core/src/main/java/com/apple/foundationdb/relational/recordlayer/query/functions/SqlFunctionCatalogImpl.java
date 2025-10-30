/*
 * SqlFunctionCatalog.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CatalogedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.BuiltInFunctionCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

/**
 * A catalog of built-in and user-defined SQL functions.
 */
@API(API.Status.EXPERIMENTAL)
final class SqlFunctionCatalogImpl implements SqlFunctionCatalog {

    @Nonnull
    private static final ImmutableMap<String, Function<Integer, Optional<BuiltInFunction<? extends Typed>>>> builtInSynonyms = createSynonyms();

    @Nonnull
    private final UserDefinedFunctionCatalog userDefinedFunctionCatalog;

    private SqlFunctionCatalogImpl(boolean isCaseSensitive) {
        this.userDefinedFunctionCatalog = new UserDefinedFunctionCatalog(isCaseSensitive);
    }

    @Nonnull
    @Override
    public CatalogedFunction lookupFunction(@Nonnull final String name, @Nonnull final Expressions arguments) {
        final var builtInFunctionMaybe = lookupBuiltInFunction(name, arguments);
        final var userDefinedFunctionMaybe = lookupUserDefinedFunction(name, arguments);
        if (builtInFunctionMaybe.isPresent() && userDefinedFunctionMaybe.isPresent()) {
            // this should never happen.
            Assert.failUnchecked(ErrorCode.INTERNAL_ERROR, "multiple definition of '" + name + "' found in the catalog");
        }
        if (builtInFunctionMaybe.isEmpty() && userDefinedFunctionMaybe.isEmpty()) {
            Assert.failUnchecked(ErrorCode.UNDEFINED_FUNCTION, "could not find function '" + name + "'");
        }
        if (builtInFunctionMaybe.isPresent()) {
            return builtInFunctionMaybe.get();
        }
        return userDefinedFunctionMaybe.get();
    }

    @Nonnull
    private Optional<? extends CatalogedFunction> lookupBuiltInFunction(@Nonnull final String name,
                                                                        @Nonnull final Expressions expressions) {
        final var functionValidator = builtInSynonyms.get(name.toLowerCase(Locale.ROOT));
        if (functionValidator == null) {
            return Optional.empty();
        }
        // TODO we should streamline the validation logic.
        return functionValidator.apply(expressions.size());
    }

    @Nonnull
    private Optional<? extends CatalogedFunction> lookupUserDefinedFunction(@Nonnull final String name,
                                                                            @Nonnull final Expressions expressions) {
        return userDefinedFunctionCatalog.lookup(name, expressions);
    }

    @Override
    public boolean containsFunction(@Nonnull final String name) {
        return builtInSynonyms.containsKey(name.toLowerCase(Locale.ROOT))
                || userDefinedFunctionCatalog.containsFunction(name);
    }

    @Override
    public boolean isJavaCallFunction(@Nonnull final String name) {
        return "java_call".equals(name.trim().toLowerCase(Locale.ROOT));
    }

    public void registerUserDefinedFunction(@Nonnull final String functionName,
                                            @Nonnull final Function<Boolean, ? extends UserDefinedFunction> functionSupplier) {
        userDefinedFunctionCatalog.registerFunction(functionName, functionSupplier);
    }

    @Nonnull
    private static ImmutableMap<String, Function<Integer, Optional<BuiltInFunction<? extends Typed>>>> createSynonyms() {
        return ImmutableMap.<String, Function<Integer, Optional<BuiltInFunction<? extends Typed>>>>builder()
                .put("+", argumentsCount -> BuiltInFunctionCatalog.resolve("add", argumentsCount))
                .put("-", argumentsCount -> BuiltInFunctionCatalog.resolve("sub", argumentsCount))
                .put("*", argumentsCount -> BuiltInFunctionCatalog.resolve("mul", argumentsCount))
                .put("/", argumentsCount -> BuiltInFunctionCatalog.resolve("div", argumentsCount))
                .put("%", argumentsCount -> BuiltInFunctionCatalog.resolve("mod", argumentsCount))
                .put(">", argumentsCount -> BuiltInFunctionCatalog.resolve("gt", argumentsCount))
                .put(">=", argumentsCount -> BuiltInFunctionCatalog.resolve("gte", argumentsCount))
                .put("<", argumentsCount -> BuiltInFunctionCatalog.resolve("lt", argumentsCount))
                .put("<=", argumentsCount -> BuiltInFunctionCatalog.resolve("lte", argumentsCount))
                .put("=", argumentsCount -> BuiltInFunctionCatalog.resolve("equals", argumentsCount))
                .put("<>", argumentsCount -> BuiltInFunctionCatalog.resolve("notEquals", argumentsCount))
                .put("!=", argumentsCount -> BuiltInFunctionCatalog.resolve("notEquals", argumentsCount))
                .put("&", argumentsCount -> BuiltInFunctionCatalog.resolve("bitand", argumentsCount))
                .put("|", argumentsCount -> BuiltInFunctionCatalog.resolve("bitor", argumentsCount))
                .put("^", argumentsCount -> BuiltInFunctionCatalog.resolve("bitxor", argumentsCount))
                .put("[]", argumentsCount -> BuiltInFunctionCatalog.resolve("subscript", argumentsCount))
                .put("bitmap_bit_position", argumentsCount -> BuiltInFunctionCatalog.resolve("bitmap_bit_position", 1 + argumentsCount))
                .put("bitmap_bucket_offset", argumentsCount -> BuiltInFunctionCatalog.resolve("bitmap_bucket_offset", 1 + argumentsCount))
                .put("bitmap_construct_agg", argumentsCount -> BuiltInFunctionCatalog.resolve("BITMAP_CONSTRUCT_AGG", argumentsCount))
                .put("not", argumentsCount -> BuiltInFunctionCatalog.resolve("not", argumentsCount))
                .put("and", argumentsCount -> BuiltInFunctionCatalog.resolve("and", argumentsCount))
                .put("or", argumentsCount -> BuiltInFunctionCatalog.resolve("or", argumentsCount))
                .put("count", argumentsCount -> BuiltInFunctionCatalog.resolve("COUNT", argumentsCount))
                .put("max", argumentsCount -> BuiltInFunctionCatalog.resolve("MAX", argumentsCount))
                .put("min", argumentsCount -> BuiltInFunctionCatalog.resolve("MIN", argumentsCount))
                .put("avg", argumentsCount -> BuiltInFunctionCatalog.resolve("AVG", argumentsCount))
                .put("sum", argumentsCount -> BuiltInFunctionCatalog.resolve("SUM", argumentsCount))
                .put("max_ever", argumentsCount -> BuiltInFunctionCatalog.resolve("MAX_EVER", argumentsCount))
                .put("min_ever", argumentsCount -> BuiltInFunctionCatalog.resolve("MIN_EVER", argumentsCount))
                .put("java_call", argumentsCount -> BuiltInFunctionCatalog.resolve("java_call", argumentsCount))
                .put("greatest", argumentsCount -> BuiltInFunctionCatalog.resolve("greatest", argumentsCount))
                .put("least", argumentsCount -> BuiltInFunctionCatalog.resolve("least", argumentsCount))
                .put("like", argumentsCount -> BuiltInFunctionCatalog.resolve("like", argumentsCount))
                .put("in", argumentsCount -> BuiltInFunctionCatalog.resolve("in", argumentsCount))
                .put("coalesce", argumentsCount -> BuiltInFunctionCatalog.resolve("coalesce", argumentsCount))
                .put("is null", argumentsCount -> BuiltInFunctionCatalog.resolve("isNull", argumentsCount))
                .put("is not null", argumentsCount -> BuiltInFunctionCatalog.resolve("notNull", argumentsCount))
                .put("isdistinctfrom", argumentsCount -> BuiltInFunctionCatalog.resolve("isDistinctFrom", argumentsCount))
                .put("isnotdistinctfrom", argumentsCount -> BuiltInFunctionCatalog.resolve("notDistinctFrom", argumentsCount))
                .put("range", argumentsCount -> BuiltInFunctionCatalog.resolve("range", argumentsCount))
                .put("__pattern_for_like", argumentsCount -> BuiltInFunctionCatalog.resolve("patternForLike", argumentsCount))
                .put("__internal_array", argumentsCount -> BuiltInFunctionCatalog.resolve("array", argumentsCount))
                .put("__pick_value", argumentsCount -> BuiltInFunctionCatalog.resolve("pick", argumentsCount))
                .build();
    }

    @Nonnull
    public static SqlFunctionCatalogImpl newInstance(@Nonnull final RecordLayerSchemaTemplate metadata,
                                                     boolean isCaseSensitive) {
        final var functionCatalog = new SqlFunctionCatalogImpl(isCaseSensitive);
        metadata.getInvokedRoutines().forEach(func ->
                functionCatalog.registerUserDefinedFunction(
                        Assert.notNullUnchecked(func.getName()),
                        func.getUserDefinedFunctionProvider()));
        return functionCatalog;
    }
}
