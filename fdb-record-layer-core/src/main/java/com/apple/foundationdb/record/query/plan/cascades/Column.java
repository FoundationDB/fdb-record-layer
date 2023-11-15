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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A class to hold a value computing a column together with information about the field of a record it supplies
 * the result for.
 * @param <V> type parameter for the {@link Value}
 */
public class Column<V extends Value> implements PlanHashable {
    @Nonnull
    private final Field field;
    @Nonnull
    private final V value;

    @Nonnull
    private final boolean expandRecord;

    private Column(@Nonnull final Field field, @Nonnull final V value, final boolean expandRecord) {
        this.field = field;
        this.value = value;
        Verify.verify(!expandRecord || value.getResultType().isRecord());
        Verify.verify(!expandRecord || field.getFieldNameOptional().isEmpty());
        this.expandRecord = expandRecord;
    }

    @Nonnull
    public Field getField() {
        return field;
    }

    @Nonnull
    public V getValue() {
        return value;
    }

    public boolean isExpandRecord() {
        return expandRecord;
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
        return getField().equals(column.getField()) && getValue().equals(column.getValue()) && isExpandRecord() == column.isExpandRecord();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getField(), getValue(), isExpandRecord());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        // plan hash everything substantial except the field result type
        return PlanHashable.objectsPlanHash(hashMode, getField().getFieldNameOptional(),
                getField().getFieldIndexOptional(), getValue(), isExpandRecord());
    }

    @Nonnull
    public static <V extends Value> Column<V> unnamedOf(@Nonnull final V value) {
        return unnamedOf(value, false);
    }

    @Nonnull
    public static <V extends Value> Column<V> unnamedOf(@Nonnull final V value, final boolean expandRecord) {
        return of(Field.unnamedOf(value.getResultType()), value, expandRecord);
    }

    @Nonnull
    public static <V extends Value> Column<V> of(@Nonnull final Field field, @Nonnull final V value) {
        return of(field, value, false);
    }

    @Nonnull
    public static <V extends Value> Column<V> of(@Nonnull final Field field, @Nonnull final V value, final boolean expandRecord) {
        return new Column<>(field, value, expandRecord);
    }
}
