/*
 * AbsoluteValueFunctionKeyExpression.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Converts an integer into its absolute value. This function is invertible in that for a given value <i>v</i>,
 * it is always possible to calculate all possible values that have an absolute value of <i>v</i>.
 */
public class AbsoluteValueFunctionKeyExpression extends InvertibleFunctionKeyExpression {
    public static final String NAME = "abs_value";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Absoluste-Value-Function");

    protected AbsoluteValueFunctionKeyExpression(@Nonnull final String name, @Nonnull final KeyExpression arguments) {
        super(name, arguments);
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> record, @Nullable final Message message, @Nonnull final Key.Evaluated arguments) {
        if (arguments.getObject(0) == null) {
            return Collections.singletonList(arguments);
        }
        long value = arguments.getLong(0);
        if (value == Long.MIN_VALUE) {
            // Math.abs(Long.MIN_VALUE) == Long.MIN_VALUE < 0,
            // which breaks assumptions people have about this function.
            // If this weren't a test-only class, perhaps we'd fix this by using
            // BigIntegers
            throw new RecordCoreArgumentException("cannot evaluate absolute value function on min long");
        }
        return Collections.singletonList(Key.Evaluated.scalar(Math.abs(value)));
    }

    @Override
    protected List<Key.Evaluated> evaluateInverseInternal(@Nonnull final Key.Evaluated result) {
        if (result.getObject(0) == null) {
            return Collections.emptyList();
        }
        long value = result.getLong(0);
        if (value < 0L) {
            return Collections.emptyList();
        } else if (value == 0L) {
            return Collections.singletonList(Key.Evaluated.scalar(0L));
        } else {
            return List.of(Key.Evaluated.scalar(value), Key.Evaluated.scalar(-1L * value));
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return super.basePlanHash(mode, BASE_HASH, arguments);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return super.baseQueryHash(hashKind, BASE_HASH, arguments);
    }

    @Override
    public int getMinArguments() {
        return 1;
    }

    @Override
    public int getMaxArguments() {
        return 1;
    }

    @Override
    public boolean isInjective() {
        return false;
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final List<? extends Value> argumentValues) {
        throw new UnsupportedOperationException("not implemented");
    }

    private static class Builder extends FunctionKeyExpression.Builder {
        public Builder() {
            super(NAME);
        }

        @Nonnull
        @Override
        public AbsoluteValueFunctionKeyExpression build(@Nonnull final KeyExpression arguments) {
            return new AbsoluteValueFunctionKeyExpression(getName(), arguments);
        }
    }

    /**
     * Factory for creating {@link AbsoluteValueFunctionKeyExpression}s.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class Factory implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return List.of(new Builder());
        }
    }
}
