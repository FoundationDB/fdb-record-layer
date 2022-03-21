/*
 * Column.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.predicates.Value;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A case class to hold a values computing a column together with information about the field of a record it supplies
 * the result for.
 * @param <V> type parameter for the {@link Value}
 */
public class Column<V extends Value> {
    @Nonnull
    private final Type.Record.Field field;
    @Nonnull
    private final V value;

    public Column(@Nonnull final Type.Record.Field field, @Nonnull final V value) {
        this.field = field;
        this.value = value;
    }

    @Nonnull
    public Type.Record.Field getField() {
        return field;
    }

    @Nonnull
    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Column)) {
            return false;
        }
        final Column<?> column = (Column<?>)o;
        return getField().equals(column.getField()) && getValue().equals(column.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getField(), getValue());
    }

    public static <V extends Value> Column<V> unnamedOf(@Nonnull final V value) {
        return new Column<>(Type.Record.Field.unnamedOf(value.getResultType()), value);
    }

    public static <V extends Value> Column<V> of(@Nonnull final Type.Record.Field field, @Nonnull final V value) {
        return new Column<>(field, value);
    }
}
