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
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * A catalog of built-in SQL functions.
 */
@API(API.Status.EXPERIMENTAL)
public final class SqlFunctionCatalog implements FunctionCatalog {

    @Nonnull
    private static final AutoServiceFunctionCatalog underlying = AutoServiceFunctionCatalog.instance();

    @Nonnull
    private static final SqlFunctionCatalog INSTANCE = new SqlFunctionCatalog();

    @Nonnull
    private final ImmutableMap<String, BuiltInFunction<? extends Typed>> synonyms;

    private SqlFunctionCatalog() {
        this.synonyms = createSynonyms();
    }

    @Nonnull
    @Override
    public BuiltInFunction<? extends Typed> lookUpFunction(@Nonnull String name) {
        return Assert.notNullUnchecked(synonyms.get(name.toLowerCase(Locale.ROOT)));
    }

    @Override
    public boolean containsFunction(@Nonnull String name) {
        return synonyms.containsKey(name.toLowerCase(Locale.ROOT));
    }

    @Nonnull
    private static ImmutableMap<String, BuiltInFunction<? extends Typed>> createSynonyms() {
        return ImmutableMap.<String, BuiltInFunction<? extends Typed>>builder()
                .put("+", underlying.lookUpFunction("add"))
                .put("-", underlying.lookUpFunction("sub"))
                .put("*", underlying.lookUpFunction("mul"))
                .put("/", underlying.lookUpFunction("div"))
                .put("%", underlying.lookUpFunction("mod"))
                .put(">", underlying.lookUpFunction("gt"))
                .put(">=", underlying.lookUpFunction("gte"))
                .put("<", underlying.lookUpFunction("lt"))
                .put("<=", underlying.lookUpFunction("lte"))
                .put("=", underlying.lookUpFunction("equals"))
                .put("<>", underlying.lookUpFunction("notequals"))
                .put("!=", underlying.lookUpFunction("notequals"))
                .put("&", underlying.lookUpFunction("bitand"))
                .put("|", underlying.lookUpFunction("bitor"))
                .put("^", underlying.lookUpFunction("bitxor"))
                .put("bitmap_bit_position", underlying.lookUpFunction("bitmap_bit_position"))
                .put("bitmap_bucket_offset", underlying.lookUpFunction("bitmap_bucket_offset"))
                .put("bitmap_construct_agg", underlying.lookUpFunction("bitmap_construct_agg"))
                .put("not", underlying.lookUpFunction("not"))
                .put("and", underlying.lookUpFunction("and"))
                .put("or", underlying.lookUpFunction("or"))
                .put("count", underlying.lookUpFunction("count"))
                .put("max", underlying.lookUpFunction("max"))
                .put("min", underlying.lookUpFunction("min"))
                .put("avg", underlying.lookUpFunction("avg"))
                .put("sum", underlying.lookUpFunction("sum"))
                .put("max_ever", underlying.lookUpFunction("max_ever"))
                .put("min_ever", underlying.lookUpFunction("min_ever"))
                .put("java_call", underlying.lookUpFunction("java_call"))
                .put("greatest", underlying.lookUpFunction("greatest"))
                .put("least", underlying.lookUpFunction("least"))
                .put("like", underlying.lookUpFunction("like"))
                .put("in", underlying.lookUpFunction("in"))
                .put("coalesce", underlying.lookUpFunction("coalesce"))
                .put("is null", underlying.lookUpFunction("isnull"))
                .put("is not null", underlying.lookUpFunction("notnull"))
                .put("__pattern_for_like", underlying.lookUpFunction("patternforlike"))
                .put("__internal_array", underlying.lookUpFunction("array"))
                .build();
    }

    @Nonnull
    public static SqlFunctionCatalog instance() {
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
                    .map(SqlFunctionCatalog::flattenRecordWithOneField).map(v -> (Value) v).collect(toList()));
        }
        return value;
    }
}
