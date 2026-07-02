/*
 * SerializedSizeFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public final class SerializedSizeFunctionKeyExpression extends FunctionKeyExpression {
    public static final String FUNCTION_NAME = "serialized_size";
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Serialized-Size-Function-Key-Expression");

    private SerializedSizeFunctionKeyExpression(@Nonnull final KeyExpression arguments) {
        super(FUNCTION_NAME, arguments);
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> record, @Nullable final Message message, @Nonnull final Key.Evaluated arguments) {
        return List.of(Key.Evaluated.scalar(extractSize(record)));
    }

    private long extractSize(@Nullable FDBRecord<?> rec) {
        if (rec instanceof FDBIndexableRecord<?> indexableRecord) {
            return indexableRecord.getValueSize();
        }
        return 0L;
    }

    @Override
    public int getMinArguments() {
        return 0;
    }

    @Override
    public int getMaxArguments() {
        return 0;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final List<? extends Value> argumentValues) {
        return resolveAndEncapsulateFunction(getName(), argumentValues);
    }

    @Override
    public boolean createsDuplicates() {
        return false;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, getName(), getArguments());
    }

    @AutoService(FunctionKeyExpression.Factory.class)
    public static final class Factory implements FunctionKeyExpression.Factory {
        private static final List<FunctionKeyExpression.Builder> builders = List.of(
                new Builder(FUNCTION_NAME) {
                    @Nonnull
                    @Override
                    public FunctionKeyExpression build(@Nonnull final KeyExpression arguments) {
                        return new SerializedSizeFunctionKeyExpression(arguments);
                    }
                }
        );

        @Nonnull
        @Override
        public List<Builder> getBuilders() {
            return builders;
        }
    }
}
