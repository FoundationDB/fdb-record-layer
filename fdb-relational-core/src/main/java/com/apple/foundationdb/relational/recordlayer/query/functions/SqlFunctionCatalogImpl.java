/*
 * SqlFunctionCatalog.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * A catalog of built-in SQL functions.
 */
@API(API.Status.EXPERIMENTAL)
public final class SqlFunctionCatalogImpl implements SqlFunctionCatalog {

    @Nonnull
    private static final SqlFunctionCatalogImpl INSTANCE = new SqlFunctionCatalogImpl();

    @Nonnull
    private final ImmutableMap<String, Function<List<Type>, BuiltInFunction<? extends Typed>>> synonyms;

    private SqlFunctionCatalogImpl() {
        this.synonyms = createSynonyms();
    }

    @Nonnull
    @Override
    public BuiltInFunction<? extends Typed> lookUpFunction(@Nonnull final String name, @Nonnull final Expressions expressions) {
        return Assert.notNullUnchecked(Objects.requireNonNull(synonyms.get(name.toLowerCase(Locale.ROOT))).apply(expressions.underlyingTypes()));
    }

    @Override
    public boolean containsFunction(@Nonnull String name) {
        return synonyms.containsKey(name.toLowerCase(Locale.ROOT));
    }

    @Override
    public boolean isUdfFunction(@Nonnull final String name) {
        return name.trim().toLowerCase(Locale.ROOT).equals("java_call");
    }

    @Nonnull
    private static ImmutableMap<String, Function<List<Type>, BuiltInFunction<? extends Typed>>> createSynonyms() {
        return ImmutableMap.<String, Function<List<Type>, BuiltInFunction<? extends Typed>>>builder()
                .put("+", types -> FunctionCatalog.resolve("add", types.size()).orElseThrow())
                .put("-", types -> FunctionCatalog.resolve("sub", types.size()).orElseThrow())
                .put("*", types -> FunctionCatalog.resolve("mul", types.size()).orElseThrow())
                .put("/", types -> FunctionCatalog.resolve("div", types.size()).orElseThrow())
                .put("%", types -> FunctionCatalog.resolve("mod", types.size()).orElseThrow())
                .put(">", types -> FunctionCatalog.resolve("gt", types.size()).orElseThrow())
                .put(">=", types -> FunctionCatalog.resolve("gte", types.size()).orElseThrow())
                .put("<", types -> FunctionCatalog.resolve("lt", types.size()).orElseThrow())
                .put("<=", types -> FunctionCatalog.resolve("lte", types.size()).orElseThrow())
                .put("=", types -> FunctionCatalog.resolve("equals", types.size()).orElseThrow())
                .put("<>", types -> FunctionCatalog.resolve("notEquals", types.size()).orElseThrow())
                .put("!=", types -> FunctionCatalog.resolve("notEquals", types.size()).orElseThrow())
                .put("&", types -> FunctionCatalog.resolve("bitand", types.size()).orElseThrow())
                .put("|", types -> FunctionCatalog.resolve("bitor", types.size()).orElseThrow())
                .put("^", types -> FunctionCatalog.resolve("bitxor", types.size()).orElseThrow())
                .put("bitmap_bit_position", types -> FunctionCatalog.resolve("bitmap_bit_position", 1 + types.size()).orElseThrow())
                .put("bitmap_bucket_offset", types -> FunctionCatalog.resolve("bitmap_bucket_offset", 1 + types.size()).orElseThrow())
                .put("bitmap_construct_agg", types -> FunctionCatalog.resolve("BITMAP_CONSTRUCT_AGG", types.size()).orElseThrow())
                .put("not", types -> FunctionCatalog.resolve("not", types.size()).orElseThrow())
                .put("and", types -> FunctionCatalog.resolve("and", types.size()).orElseThrow())
                .put("or", types -> FunctionCatalog.resolve("or", types.size()).orElseThrow())
                .put("count", types -> FunctionCatalog.resolve("COUNT", types.size()).orElseThrow())
                .put("max", types -> FunctionCatalog.resolve("MAX", types.size()).orElseThrow())
                .put("min", types -> FunctionCatalog.resolve("MIN", types.size()).orElseThrow())
                .put("avg", types -> FunctionCatalog.resolve("AVG", types.size()).orElseThrow())
                .put("sum", types -> FunctionCatalog.resolve("SUM", types.size()).orElseThrow())
                .put("max_ever", types -> FunctionCatalog.resolve("MAX_EVER", types.size()).orElseThrow())
                .put("min_ever", types -> FunctionCatalog.resolve("MIN_EVER", types.size()).orElseThrow())
                .put("java_call", types -> FunctionCatalog.resolve("java_call", types.size()).orElseThrow())
                .put("greatest", types -> FunctionCatalog.resolve("greatest", types.size()).orElseThrow())
                .put("least", types -> FunctionCatalog.resolve("least", types.size()).orElseThrow())
                .put("like", types -> FunctionCatalog.resolve("like", types.size()).orElseThrow())
                .put("in", types -> FunctionCatalog.resolve("in", types.size()).orElseThrow())
                .put("coalesce", types -> FunctionCatalog.resolve("coalesce", types.size()).orElseThrow())
                .put("is null", types -> FunctionCatalog.resolve("isNull", types.size()).orElseThrow())
                .put("is not null", types -> FunctionCatalog.resolve("notNull", types.size()).orElseThrow())
                .put("__pattern_for_like", types -> FunctionCatalog.resolve("patternForLike", types.size()).orElseThrow())
                .put("__internal_array", types -> FunctionCatalog.resolve("array", types.size()).orElseThrow())
                .build();
    }

    @Nonnull
    public static SqlFunctionCatalogImpl instance() {
        return INSTANCE;
    }

    /**
     * A utility method that transforms a single-item {@link RecordConstructorValue} value into its inner {@link Value}.
     * This is mainly used for deterministically distinguishing between:
     * <ul>
     *     <li>Single item record constructor</li>
     *     <li>Order of operations</li>
     * </ul>
     * Currently, all of our SQL functions are assumed to be working with primitives, therefore, when there is a scenario
     * where the arguments can be interpreted as either single-item records or to indicate order of operations,
     * the precedence is <i>always</i> given to the latter.
     * For example, the argument {@code (3+4)} in this expression {@code (3+4)*5} is considered to correspond to a
     * single integer which is the result of {@code 3+4} as opposed to a single-item record whose element is {@code 3+4}.
     *
     * @param value The value to potentially simplify
     * @return if the {@code value} is a single-item record, then the content of the {@code value} is recursively checked
     * and returned, otherwise, the {@code value} itself is returned without modification.
     */
    @Nonnull
    public static Typed flattenRecordWithOneField(@Nonnull final Typed value) {
        if (value instanceof RecordConstructorValue && ((RecordConstructorValue) value).getColumns().size() == 1) {
            return flattenRecordWithOneField(((Value) value).getChildren().iterator().next());
        }
        if (value instanceof Value) {
            return ((Value) value).withChildren(StreamSupport.stream(((Value) value).getChildren().spliterator(), false)
                    .map(SqlFunctionCatalogImpl::flattenRecordWithOneField).map(v -> (Value) v).collect(toList()));
        }
        return value;
    }
}
