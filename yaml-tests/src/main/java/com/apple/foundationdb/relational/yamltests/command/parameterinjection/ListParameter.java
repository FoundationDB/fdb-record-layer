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
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import org.jspecify.annotations.Nullable;
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

    private final List<Parameter> values;

    public ListParameter(List<Parameter> values) {
        this.values = values;
    }

    @Override
    public ListParameter bind(Random random) {
        if (!isUnbound()) {
            return this;
        }
        return new ListParameter(values.stream().map(v -> v.bind(random)).collect(Collectors.toList()));
    }

    @Override
    public boolean isUnbound() {
        return values.stream().anyMatch(Parameter::isUnbound);
    }

    List<Parameter> getValues() {
        return this.values;
    }

    @Override
    @Nullable
    @SuppressWarnings("NullAway") // NullAway/JSpecify does not currently track nullability of array element writes;
    // array legitimately holds SQL NULL entries (see the Objects::nonNull filtering below), same as before this migration.
    @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
            justification = "connection is @Nullable to match the wider Parameter#getSqlObject contract (other implementations, e.g. UnboundParameter, tolerate a null connection); this implementation happens to always need a real Connection to build the java.sql.Array.")
    public Object getSqlObject(@Nullable Connection connection) throws SQLException {
        ensureBoundedness();
        var array = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = values.get(i).getSqlObject(connection);
        }
        return Objects.requireNonNull(connection).createArrayOf(getSqlTypeName(array), array);
    }

    // Best-effort approach to determine the type of constituent elements.
    private String getSqlTypeName(Object[] array) {
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
        } else if (firstNonNull instanceof Boolean) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.BOOLEAN);
        } else if (firstNonNull instanceof Double) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.DOUBLE);
        } else if (firstNonNull instanceof Float) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.FLOAT);
        } else if (firstNonNull instanceof String) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.VARCHAR);
        } else if (firstNonNull instanceof byte[]) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.BINARY);
        } else if (firstNonNull instanceof Array) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.ARRAY);
        } else if (firstNonNull instanceof Struct) {
            return SqlTypeNamesSupport.getSqlTypeName(Types.STRUCT);
        }
        throw Assert.failUnchecked("ListParameter does not support array of type: " + array[0].getClass().getSimpleName());
    }

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
