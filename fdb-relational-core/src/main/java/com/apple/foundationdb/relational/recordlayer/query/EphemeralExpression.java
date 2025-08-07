/*
 * EphemeralExpression.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * An expression that is used mainly for alias resolution and does not materialize into an operator output.
 */
@API(API.Status.EXPERIMENTAL)
public class EphemeralExpression extends Expression {

    public EphemeralExpression(@Nonnull Identifier name, @Nonnull DataType dataType, @Nonnull Value expression) {
        super(Optional.of(name), dataType, expression);
    }

    @Nonnull
    @Override
    public EphemeralExpression asEphemeral() {
        return this;
    }
}
