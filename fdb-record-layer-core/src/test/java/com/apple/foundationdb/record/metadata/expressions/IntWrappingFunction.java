/*
 * IntWrappingFunction.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Wraps a integer into a string. Integers are wrapped into strings by appending a prefix. This function
 * is invertible, as strings with the prefix can be unwrapped and the original integer value recovered.
 */
public class IntWrappingFunction extends InvertibleFunctionKeyExpression {
    public static final String NAME = "wrap_int";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Int-Wrapping-Function");
    private static final String PREFIX = "i:";

    protected IntWrappingFunction(@Nonnull final String name, @Nonnull final KeyExpression arguments) {
        super(name, arguments);
    }

    @Override
    public boolean isInjective() {
        return true;
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

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> record,
                                                                    @Nullable final Message message,
                                                                    @Nonnull final Key.Evaluated arguments) {
        return Collections.singletonList(Key.Evaluated.scalar(PREFIX + arguments.getLong(0)));
    }

    @Override
    protected List<Key.Evaluated> evaluateInverseInternal(@Nonnull final Key.Evaluated result) {
        String canonicalForm = result.getString(0);
        if (canonicalForm != null && canonicalForm.startsWith(PREFIX)) {
            return Collections.singletonList(Key.Evaluated.scalar(Long.parseLong(canonicalForm, PREFIX.length(), canonicalForm.length(), 10)));
        } else {
            return Collections.singletonList(result);
        }
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    private static class Builder extends FunctionKeyExpression.Builder {
        public Builder() {
            super(NAME);
        }

        @Nonnull
        @Override
        public FunctionKeyExpression build(@Nonnull final KeyExpression arguments) {
            return new IntWrappingFunction(getName(), arguments);
        }
    }

    /**
     * Factory for creating {@link IntWrappingFunction}s.
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
