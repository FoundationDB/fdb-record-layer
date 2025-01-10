/*
 * ParsingFragment.java
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

package com.apple.foundationdb.relational.api.fluentsql.expression;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.fluentsql.FluentVisitor;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This represents an opaque query fragment, this is usually used in two situations.
 * <ul>
 *     <li>
 *         Building structured query from a given SQL statement: the builder uses {@code ParsingFragment}
 *         to save chunks of the query string without going deeper into processing them and modelling them into a corresponding
 *         {@code Expression<?>} tree, this is required to capture expressions that can not be represented at compile time
 *         such as complex boolean expression trees in {@code where} clause, or to "glance over" certain parts of the query
 *         that are irrelevant for the builder for performance reasons.
 *     </li>
 *     <li>
 *         Query decoration: The user wants to decorate the query with expressions not (yet) supported by the {@link ExpressionFactory}.
 *     </li>
 * </ul>
 * @param <T> The type of the fragment.
 */
@API(API.Status.EXPERIMENTAL)
public class ParsingFragment<T extends DataType> implements ExpressionFragment<T> {
    @Nonnull
    private final String fragment;

    @Nonnull
    private final T dataType;

    public ParsingFragment(@Nonnull final T dataType,
                           @Nonnull final String fragment) {
        this.dataType = dataType;
        this.fragment = fragment;
    }

    @Nullable
    @Override
    public <R, C> R accept(@Nonnull FluentVisitor<R, C> visitor, @Nonnull C context) {
        return visitor.visit(this, context);
    }

    @Override
    public DataType getType() {
        return dataType;
    }

    @Nonnull
    @Override
    public String getFragment() {
        return fragment;
    }

    @Override
    public String toString() {
        return "{" + fragment + "} : " + dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ParsingFragment<?> that = (ParsingFragment<?>) o;
        return Objects.equals(fragment, that.fragment) && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, dataType);
    }
}
