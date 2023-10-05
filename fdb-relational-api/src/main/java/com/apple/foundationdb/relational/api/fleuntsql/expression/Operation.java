/*
 * Operation.java
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

package com.apple.foundationdb.relational.api.fleuntsql.expression;

import javax.annotation.Nonnull;

/**
 * This is a placeholder of all supported operations.
 */
public enum Operation {

    EQUAL(Boolean.class),
    NOT_EQUAL(Boolean.class),
    IS_NULL(Boolean.class),
    IS_NOT_NULL(Boolean.class),
    GREATER_THAN(Boolean.class),
    LESS_THAN(Boolean.class),
    GREATER_THAN_EQUALS(Boolean.class),
    LESS_THAN_EQUALS(Boolean.class),
    AND(Boolean.class),
    OR(Boolean.class),
    NOT(Boolean.class),
    ADD(Number.class),
    DIV(Number.class),
    MUL(Number.class),
    SUB(Number.class),
    MOD(Number.class),
    GREATEST(Comparable.class),
    JAVA_CALL(Object.class);

    @Nonnull
    private final Class<?> type;

    Operation(@Nonnull Class<?> type) {
        this.type = type;
    }

    @Nonnull
    public Class<?> getType() {
        return type;
    }
}
