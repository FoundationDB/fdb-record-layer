/*
 * SingleNestedContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;

import javax.annotation.Nonnull;

/**
 * An implementation of the {@code NestingContext} interface for non-repeated nested expressions, such as
 * {@link com.apple.foundationdb.record.query.expressions.NestedField} and
 * {@link com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression} on a field with a
 * {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType} of {@code NONE}.
 */
@API(API.Status.EXPERIMENTAL)
public class SingleNestedContext implements NestedContext {
    @Nonnull
    private final FieldKeyExpression parentField;

    public SingleNestedContext(@Nonnull String parentFieldName) {
        this.parentField = Key.Expressions.field(parentFieldName);
    }

    public SingleNestedContext(@Nonnull FieldKeyExpression parentField) {
        this.parentField = parentField;
    }

    @Nonnull
    @Override
    public FieldKeyExpression getParentField() {
        return parentField;
    }

    @Override
    public boolean isParentFieldFannedOut() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SingleNestedContext that = (SingleNestedContext)o;

        return parentField.equals(that.parentField);
    }

    @Override
    public int hashCode() {
        return parentField.hashCode();
    }
}
