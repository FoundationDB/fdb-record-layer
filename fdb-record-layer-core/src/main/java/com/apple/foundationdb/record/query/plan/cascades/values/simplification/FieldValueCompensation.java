/*
 * FieldValueCompensation.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * A compensation that utilizes a field access.
 */
public class FieldValueCompensation implements Function<Value, Value> {
    @Nonnull
    private final FieldValue.FieldPath fieldPath;

    @Nonnull
    private final Function<Value, Value> downstreamCompensation;

    public FieldValueCompensation(@Nonnull final FieldValue.FieldPath fieldPath) {
        this(fieldPath, Function.identity());
    }

    public FieldValueCompensation(@Nonnull final FieldValue.FieldPath fieldPath, @Nonnull final Function<Value, Value> downstreamCompensation) {
        this.fieldPath = fieldPath;
        this.downstreamCompensation = downstreamCompensation;
    }


    @Nonnull
    public FieldValue.FieldPath getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    @Override
    public Value apply(final Value value) {
        return downstreamCompensation.apply(FieldValue.ofFieldsAndFuseIfPossible(value, fieldPath));
    }

    public Function<Value, Value> withSuffix(@Nonnull final FieldValue.FieldPath suffixFieldPath) {
        if (suffixFieldPath.isEmpty()) {
            return downstreamCompensation;
        }
        return new FieldValueCompensation(suffixFieldPath, downstreamCompensation);
    }
}
