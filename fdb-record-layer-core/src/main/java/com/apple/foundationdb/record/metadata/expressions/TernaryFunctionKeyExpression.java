/*
 * TernaryFunctionKeyExpression.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public final class TernaryFunctionKeyExpression extends FunctionKeyExpression {
    @Nonnull
    public static final String FUNCTION_NAME = "if";
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Ternary-Function-Key-Expression");

    private final int columnSize;

    public TernaryFunctionKeyExpression(@Nonnull KeyExpression arguments, int columnSize) {
        super(FUNCTION_NAME, arguments);
        this.columnSize = columnSize;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> record, @Nullable final Message message, @Nonnull final Key.Evaluated arguments) {
        final Object condition = arguments.getObject(0);
        if (condition == null) {
            return List.of();
        } else if (condition instanceof List<?> listCondition) {
            final Object conditionValue = listCondition.get(0);
            if (conditionValue == null) {
                return List.of();
            } else if (conditionValue instanceof Boolean booleanCondition) {
                Object resultObject = booleanCondition ? arguments.getObject(1) : arguments.getObject(2);
                if (resultObject instanceof List<?> resultList) {
                    return List.of(Key.Evaluated.concatenate((List<Object>) resultList));
                } else {
                    return List.of(Key.Evaluated.scalar(resultObject));
                }
            } else {
                throw new RecordCoreException("Unable to evaluate ternary function as condition is non-Boolean");
            }
        } else {
            throw new RecordCoreException("Unable to evaluate ternary function as condition is non-Boolean");
        }
    }

    @Override
    public int getMinArguments() {
        return 3;
    }

    @Override
    public int getMaxArguments() {
        return 3;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final List<? extends Value> argumentValues) {
        return resolveAndEncapsulateFunction(getName(), argumentValues);
    }

    @Override
    public boolean createsDuplicates() {
        return arguments.createsDuplicates();
    }

    @Override
    public int getColumnSize() {
        return columnSize;
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
                        if (arguments instanceof ListKeyExpression listArguments) {
                            final List<KeyExpression> children = listArguments.getChildren();
                            if (children.size() != 3) {
                                throw new MetaDataException("cannot create ternary expression over non-three number of children")
                                        .addLogInfo(LogMessageKeys.KEY_EXPRESSION, arguments);
                            }
                            final KeyExpression leftExpression = children.get(1);
                            final KeyExpression rightExpression = children.get(2);
                            if (leftExpression.getColumnSize() != rightExpression.getColumnSize()) {
                                throw new MetaDataException("left and right arguments of ternary expression must have the same result column sizes")
                                        .addLogInfo(LogMessageKeys.KEY_EXPRESSION, arguments);
                            }
                            return new TernaryFunctionKeyExpression(arguments, leftExpression.getColumnSize());
                        } else {
                            throw new MetaDataException("cannot create ternary expression over non-list expression arguments")
                                    .addLogInfo(LogMessageKeys.KEY_EXPRESSION, arguments);
                        }
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
