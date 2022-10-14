/*
 * InvertibleFunctionKeyExpression.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Key expression representing an invertible function. A function is invertible if it is possible to determine
 * the pre-image of function application given the result. For example, if the function represents the squaring
 * function, then the inverse would represent the square root. Note that this class allows the inverse to return
 * multiple values, if there are multiple values that may map to the same output.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class InvertibleFunctionKeyExpression extends FunctionKeyExpression {
    protected InvertibleFunctionKeyExpression(@Nonnull final String name, @Nonnull final KeyExpression arguments) {
        super(name, arguments);
    }

    /**
     * Return all possible pre-images for the given function result.
     *
     * @param result the output from function execution
     * @return all function inputs that could result in the given output
     */
    public List<Key.Evaluated> evaluateInverse(@Nonnull Key.Evaluated result) {
        validateResultColumns(result);
        return evaluateInverseInternal(result);
    }

    protected abstract List<Key.Evaluated> evaluateInverseInternal(@Nonnull Key.Evaluated result);

    /**
     * Whether the function key expression is injective. A function is injective if and only if
     * for every function output, there is only one unique function input that maps to that output.
     * So, for example, the square function is not injective (as the inverse is both the positive
     * and negative square root), but the negation function is injective. If this
     * returns {@code true}, then {@link #evaluateInverse(Key.Evaluated)} should always return a
     * list of size 1. If this returns {@code false}, it may return more elements.
     *
     * @return whether this function is injective and thus whether the inverse should never return
     *     multiple values
     */
    public abstract boolean isInjective();

    private void validateResultColumns(@Nonnull Key.Evaluated result) {
        if (result.size() != getColumnSize()) {
            throw new RecordCoreArgumentException("result has unexpected number of columns")
                    .addLogInfo(LogMessageKeys.EXPECTED, getColumnSize())
                    .addLogInfo(LogMessageKeys.ACTUAL, result.size());
        }
    }
}
