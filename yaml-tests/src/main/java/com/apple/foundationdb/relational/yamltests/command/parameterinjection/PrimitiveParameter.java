/*
 * PrimitiveParameter.java
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
import java.util.Random;

/**
 * {@link PrimitiveParameter} holds value of a primitive type and hence is bound.
 */
public class PrimitiveParameter implements Parameter {
    @Nullable
    private final Object object;

    public PrimitiveParameter(@Nullable Object object) {
        this.object = object;
    }

    @Nonnull
    @Override
    public String getString() {
        if (object == null) {
            return "null";
        } else if (object instanceof String) {
            return "'" + object + "'";
        } else {
            return object.toString();
        }
    }

    @Nonnull
    @Override
    public PrimitiveParameter bind(@Nonnull Random random) {
        return this;
    }

    @Override
    public boolean isUnbound() {
        return false;
    }

    @Nullable
    @Override
    public Object getSqlObject(@Nullable Connection connection) {
        return object;
    }
}
