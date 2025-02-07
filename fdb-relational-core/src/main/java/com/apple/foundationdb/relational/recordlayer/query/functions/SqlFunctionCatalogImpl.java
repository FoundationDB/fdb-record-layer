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
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
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
    private final ImmutableMap<String, Function<Integer, BuiltInFunction<? extends Typed>>> synonyms;

    private SqlFunctionCatalogImpl() {
        this.synonyms = createSynonyms();
    }

    @Nonnull
    @Override
    public BuiltInFunction<? extends Typed> lookUpFunction(@Nonnull final String name, @Nonnull final Expression... expressions) {
        return Assert.notNullUnchecked(Objects.requireNonNull(synonyms.get(name.toLowerCase(Locale.ROOT))).apply(expressions.length));
    }

    @Override
    public boolean containsFunction(@Nonnull String name) {
        return synonyms.containsKey(name.toLowerCase(Locale.ROOT));
    }

    @Override
    public boolean isUdfFunction(@Nonnull final String name) {
        return "java_call".equals(name.trim().toLowerCase(Locale.ROOT));
    }

    @Nonnull
    private static ImmutableMap<String, Function<Integer, BuiltInFunction<? extends Typed>>> createSynonyms() {
        return ImmutableMap.<String, Function<Integer, BuiltInFunction<? extends Typed>>>builder()
                .put("+", argumentsCount -> FunctionCatalog.resolve("add", argumentsCount).orElseThrow())
                .put("-", argumentsCount -> FunctionCatalog.resolve("sub", argumentsCount).orElseThrow())
                .put("*", argumentsCount -> FunctionCatalog.resolve("mul", argumentsCount).orElseThrow())
                .put("/", argumentsCount -> FunctionCatalog.resolve("div", argumentsCount).orElseThrow())
                .put("%", argumentsCount -> FunctionCatalog.resolve("mod", argumentsCount).orElseThrow())
                .put(">", argumentsCount -> FunctionCatalog.resolve("gt", argumentsCount).orElseThrow())
                .put(">=", argumentsCount -> FunctionCatalog.resolve("gte", argumentsCount).orElseThrow())
                .put("<", argumentsCount -> FunctionCatalog.resolve("lt", argumentsCount).orElseThrow())
                .put("<=", argumentsCount -> FunctionCatalog.resolve("lte", argumentsCount).orElseThrow())
                .put("=", argumentsCount -> FunctionCatalog.resolve("equals", argumentsCount).orElseThrow())
                .put("<>", argumentsCount -> FunctionCatalog.resolve("notEquals", argumentsCount).orElseThrow())
                .put("!=", argumentsCount -> FunctionCatalog.resolve("notEquals", argumentsCount).orElseThrow())
                .put("&", argumentsCount -> FunctionCatalog.resolve("bitand", argumentsCount).orElseThrow())
                .put("|", argumentsCount -> FunctionCatalog.resolve("bitor", argumentsCount).orElseThrow())
                .put("^", argumentsCount -> FunctionCatalog.resolve("bitxor", argumentsCount).orElseThrow())
                .put("bitmap_bit_position", argumentsCount -> FunctionCatalog.resolve("bitmap_bit_position", 1 + argumentsCount).orElseThrow())
                .put("bitmap_bucket_offset", argumentsCount -> FunctionCatalog.resolve("bitmap_bucket_offset", 1 + argumentsCount).orElseThrow())
                .put("bitmap_construct_agg", argumentsCount -> FunctionCatalog.resolve("BITMAP_CONSTRUCT_AGG", argumentsCount).orElseThrow())
                .put("not", argumentsCount -> FunctionCatalog.resolve("not", argumentsCount).orElseThrow())
                .put("and", argumentsCount -> FunctionCatalog.resolve("and", argumentsCount).orElseThrow())
                .put("or", argumentsCount -> FunctionCatalog.resolve("or", argumentsCount).orElseThrow())
                .put("count", argumentsCount -> FunctionCatalog.resolve("COUNT", argumentsCount).orElseThrow())
                .put("max", argumentsCount -> FunctionCatalog.resolve("MAX", argumentsCount).orElseThrow())
                .put("min", argumentsCount -> FunctionCatalog.resolve("MIN", argumentsCount).orElseThrow())
                .put("avg", argumentsCount -> FunctionCatalog.resolve("AVG", argumentsCount).orElseThrow())
                .put("sum", argumentsCount -> FunctionCatalog.resolve("SUM", argumentsCount).orElseThrow())
                .put("max_ever", argumentsCount -> FunctionCatalog.resolve("MAX_EVER", argumentsCount).orElseThrow())
                .put("min_ever", argumentsCount -> FunctionCatalog.resolve("MIN_EVER", argumentsCount).orElseThrow())
                .put("java_call", argumentsCount -> FunctionCatalog.resolve("java_call", argumentsCount).orElseThrow())
                .put("greatest", argumentsCount -> FunctionCatalog.resolve("greatest", argumentsCount).orElseThrow())
                .put("least", argumentsCount -> FunctionCatalog.resolve("least", argumentsCount).orElseThrow())
                .put("like", argumentsCount -> FunctionCatalog.resolve("like", argumentsCount).orElseThrow())
                .put("in", argumentsCount -> FunctionCatalog.resolve("in", argumentsCount).orElseThrow())
                .put("coalesce", argumentsCount -> FunctionCatalog.resolve("coalesce", argumentsCount).orElseThrow())
                .put("is null", argumentsCount -> FunctionCatalog.resolve("isNull", argumentsCount).orElseThrow())
                .put("is not null", argumentsCount -> FunctionCatalog.resolve("notNull", argumentsCount).orElseThrow())
                .put("__pattern_for_like", argumentsCount -> FunctionCatalog.resolve("patternForLike", argumentsCount).orElseThrow())
                .put("__internal_array", argumentsCount -> FunctionCatalog.resolve("array", argumentsCount).orElseThrow())
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
