/*
 * FunctionCatalog.java
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

import com.apple.foundationdb.record.query.plan.cascades.CatalogedFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;

import javax.annotation.Nonnull;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * A catalog of scalar, and table-valued functions that are either built-in or user-defined.
 */
public interface SqlFunctionCatalog {

    /**
     * lookup a function in the catalog using its name, and a list of arguments. note that calling this method
     * can trigger the compilation of a user-defined function iff the user-defined function has a matching name, and this
     * is the first time it was looked up, i.e. the compilation is lazily, and only once.
     * @param name The name of the function.
     * @param arguments The arguments passed with the invocation of the function.
     * @return the function instance.
     */
    @Nonnull
    CatalogedFunction lookupFunction(@Nonnull String name, @Nonnull Expressions arguments);

    /**
     * Checks whether a function exists in the catalog. Note that invoking this method shall not trigger compiling
     * any user-defined functions.
     * @param name The name of the function.
     * @return {@code True} if the function exists, otherwise {@code false}.
     */
    boolean containsFunction(@Nonnull String name);

    // TODO: this will be removed once we unify both Java- and SQL-UDFs.
    boolean isJavaCallFunction(@Nonnull String name);

    /**
     * Returns an instance of a {@link SqlFunctionCatalogImpl} with the following functions:
     * <li>
     *     <ul>built-in scalar and table-valued functions, compiled, and SQL-aliased</ul>
     *     <ul>user-defined table-valued functions, lazily-compiled</ul>
     * </li>
     * The user-defined functions are loaded from the passed {@code metadata} argument, they are only compiled when
     * looked up with {@link SqlFunctionCatalog#lookupFunction(String, Expressions)}, and their compiled version is
     * cached so it is done at most once.
     * @param metadata The metadata used to load any user-defined functions.
     * @return a new instance of {@link SqlFunctionCatalogImpl}.
     */
    @Nonnull
    static SqlFunctionCatalog newInstance(@Nonnull final RecordLayerSchemaTemplate metadata) {
        return SqlFunctionCatalogImpl.newInstance(metadata);
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
    static Typed flattenRecordWithOneField(@Nonnull final Typed value) {
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
