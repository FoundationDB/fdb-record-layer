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
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
    private static final ImmutableMap<String, Function<Integer, Optional<BuiltInFunction<Value>>>> builtInSynonyms = createSynonyms();

    @Nonnull
    private final UserDefinedFunctionCatalog userDefinedFunctionCatalog;

    private SqlFunctionCatalogImpl(boolean isCaseSensitive) {
        this.userDefinedFunctionCatalog = new UserDefinedFunctionCatalog(isCaseSensitive);
    }

    @Nonnull
    @Override
    public CatalogedFunction<Value> lookupFunction(@Nonnull final String name, @Nonnull final Expressions arguments) {
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
    private Optional<? extends CatalogedFunction<Value>> lookupBuiltInFunction(@Nonnull final String name,
                                                                        @Nonnull final Expressions expressions) {
        final var functionValidator = builtInSynonyms.get(name.toLowerCase(Locale.ROOT));
        if (functionValidator == null) {
            return Optional.empty();
        }
        // TODO we should streamline the validation logic.
        return functionValidator.apply(expressions.size());
    }

    @Nonnull
    private Optional<? extends CatalogedFunction<Value>> lookupUserDefinedFunction(@Nonnull final String name,
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
    private static ImmutableMap<String, Function<Integer, Optional<BuiltInFunction<Value>>>> createSynonyms() {
        return ImmutableMap.<String, Function<Integer, Optional<BuiltInFunction<Value>>>>builder()
                .put("+", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("add", argumentsCount))
                .put("-", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("sub", argumentsCount))
                .put("*", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("mul", argumentsCount))
                .put("/", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("div", argumentsCount))
                .put("%", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("mod", argumentsCount))
                .put(">", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("gt", argumentsCount))
                .put(">=", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("gte", argumentsCount))
                .put("<", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("lt", argumentsCount))
                .put("<=", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("lte", argumentsCount))
                .put("=", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("equals", argumentsCount))
                .put("<>", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("notEquals", argumentsCount))
                .put("!=", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("notEquals", argumentsCount))
                .put("&", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("bitand", argumentsCount))
                .put("|", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("bitor", argumentsCount))
                .put("^", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("bitxor", argumentsCount))
                .put("[]", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("subscript", argumentsCount))
                .put("bitmap_bit_position", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("bitmap_bit_position", 1 + argumentsCount))
                .put("bitmap_bucket_offset", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("bitmap_bucket_offset", 1 + argumentsCount))
                .put("bitmap_construct_agg", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("BITMAP_CONSTRUCT_AGG", argumentsCount))
                .put("euclidean_distance", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("euclidean_distance", argumentsCount))
                .put("euclidean_square_distance", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("euclidean_square_distance", argumentsCount))
                .put("cosine_distance", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("cosine_distance", argumentsCount))
                .put("dot_product_distance", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("dot_product_distance", argumentsCount))
                .put("not", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("not", argumentsCount))
                .put("and", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("and", argumentsCount))
                .put("or", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("or", argumentsCount))
                .put("count", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("COUNT", argumentsCount))
                .put("max", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("MAX", argumentsCount))
                .put("min", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("MIN", argumentsCount))
                .put("avg", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("AVG", argumentsCount))
                .put("sum", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("SUM", argumentsCount))
                .put("max_ever", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("MAX_EVER", argumentsCount))
                .put("min_ever", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("MIN_EVER", argumentsCount))
                .put("java_call", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("java_call", argumentsCount))
                .put("greatest", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("greatest", argumentsCount))
                .put("least", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("least", argumentsCount))
                .put("like", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("like", argumentsCount))
                .put("in", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("in", argumentsCount))
                .put("coalesce", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("coalesce", argumentsCount))
                .put("is null", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("isNull", argumentsCount))
                .put("is not null", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("notNull", argumentsCount))
                .put("isdistinctfrom", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("isDistinctFrom", argumentsCount))
                .put("isnotdistinctfrom", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("notDistinctFrom", argumentsCount))
                .put("range", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("range", argumentsCount))
                .put("row_number", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("row_number", argumentsCount))
                .put("__pattern_for_like", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("patternForLike", argumentsCount))
                .put("__internal_array", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("array", argumentsCount))
                .put("__pick_value", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("pick", argumentsCount))
                .put("get_versionstamp_incarnation", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("get_versionstamp_incarnation", argumentsCount))
                .put("cardinality", argumentsCount -> FunctionCatalog.resolveBuiltInFunction("cardinality", argumentsCount))
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
