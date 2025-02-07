/*
 * ListParameter.java
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

package com.apple.foundationdb.relational.yamltests.command.parameterinjection;

import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * {@link ListParameter} holds a list of {@link Parameter}s. It is said to be bound if all its constituent
 * parameters are bound.
 */
public class ListParameter implements Parameter {

    @Nonnull
    private final List<Parameter> values;

    public ListParameter(@Nonnull List<Parameter> values) {
        this.values = values;
    }

    @Nonnull
    @Override
    public ListParameter bind(@Nonnull Random random) {
        if (!isUnbound()) {
            return this;
        }
        return new ListParameter(values.stream().map(v -> v.bind(random)).collect(Collectors.toList()));
    }

    @Override
    public boolean isUnbound() {
        return values.stream().anyMatch(Parameter::isUnbound);
    }

    @Nonnull
    List<Parameter> getValues() {
        return this.values;
    }

    @Override
    @Nullable
    public Object getSqlObject(Connection connection) throws SQLException {
        ensureBoundedness();
        var array = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = values.get(i).getSqlObject(connection);
        }
        return Objects.requireNonNull(connection).createArrayOf(getSqlTypeName(array), array);
    }

    // Best-effort approach to determine the type of constituent elements.
    @Nonnull
    private String getSqlTypeName(@Nonnull Object[] array) {
        if (array.length == 0) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.NULL);
        }
        final var allNonNulls = Arrays.stream(array).filter(Objects::nonNull).collect(Collectors.toList());
        Assert.thatUnchecked(allNonNulls.stream().map(Object::getClass).distinct().count() == 1, "Cannot assert a common type for list elements");
        final var firstNonNull = allNonNulls.get(0);
        if (firstNonNull instanceof Integer) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.INTEGER);
        } else if (firstNonNull instanceof Long) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.BIGINT);
        } else if (firstNonNull instanceof Double) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.DOUBLE);
        } else if (firstNonNull instanceof String) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.VARCHAR);
        } else if (firstNonNull instanceof Array) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.ARRAY);
        } else if (firstNonNull instanceof Struct) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.STRUCT);
        }
        Assert.failUnchecked("ListParameter does not support array of type: " + array[0].getClass().getSimpleName());
        return null;
    }

    @Nonnull
    @Override
    public String getSqlText() {
        ensureBoundedness();
        return "[" + values.stream().map(Parameter::getSqlText).collect(Collectors.joining(", ")) + "]";
    }

    @Override
    public String toString() {
        return getSqlText();
    }
}
