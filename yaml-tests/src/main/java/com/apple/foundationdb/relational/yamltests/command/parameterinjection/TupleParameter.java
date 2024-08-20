/*
 * TupleParameter.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * {@link TupleParameter} is extension to {@link ListParameter} that differ with the parent in implementation of
 * {@link Parameter#getSqlObject} and {@link Parameter#getSqlText}.
 */
public class TupleParameter extends ListParameter {

    public TupleParameter(@Nonnull List<Parameter> value) {
        super(value);
    }

    @Nonnull
    @Override
    public TupleParameter bind(@Nonnull Random random) {
        if (!isUnbound()) {
            return this;
        }
        return new TupleParameter(getValues().stream().map(v -> v.bind(random)).collect(Collectors.toList()));
    }

    @Override
    @Nullable
    public Object getSqlObject(Connection connection) throws SQLException {
        ensureBoundedness();
        var array = new Object[getValues().size()];
        for (int i = 0; i < getValues().size(); i++) {
            array[i] = getValues().get(i).getSqlObject(connection);
        }
        // Currently, the name of the struct is not set properly since we do not look at the name while creating structs.
        // This should change when the underlying assumption changes.
        return (Objects.requireNonNull(connection).createStruct("na", array));
    }

    @Nonnull
    @Override
    public String getSqlText() {
        ensureBoundedness();
        return "(" + getValues().stream().map(Parameter::getSqlText).collect(Collectors.joining(", ")) + ")";
    }
}
