/*
 * InListParameter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.util.Assert;

import org.junit.jupiter.api.Assumptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.stream.Collectors;

public class InListParameter implements Parameter {

    @Nonnull
    private final Parameter parameter;

    public InListParameter(@Nonnull Parameter parameter) {
        this.parameter = parameter;
    }

    @Nonnull
    @Override
    public InListParameter bind(@Nonnull Random random) {
        if (!isUnbound()) {
            return this;
        }
        return new InListParameter(parameter.bind(random));
    }

    @Override
    public boolean isUnbound() {
        return parameter.isUnbound();
    }

    @Override
    @Nullable
    public Object getSqlObject(@Nullable Connection connection) throws SQLException {
        ensureBoundedness();
        Assert.thatUnchecked(parameter instanceof ListParameter, "InListParameter can only have ListParameter bounded argument.");
        return parameter.getSqlObject(connection);
    }

    @Nonnull
    @Override
    public String getSqlText() {
        ensureBoundedness();
        var values = ((ListParameter) parameter).getValues();
        // TODO (execute simple statement does not work with empty in-list)
        Assumptions.assumeTrue(!values.isEmpty(), "Empty inLists are not allowed in simple statements as they would evaluate to empty resultSet.");
        return "(" + values.stream().map(Parameter::getSqlText).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String toString() {
        return "inList(" + parameter + ")";
    }
}
