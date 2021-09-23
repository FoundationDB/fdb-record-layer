/*
 * BasePlanDebugger.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.debug;

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.IntUnaryOperator;

public abstract class BasePlanDebugger implements Debugger {

    /**
     * Abstract method to return the current state of the debugger implementation.
     * @return the current {@link State} of the debugger
     */
    @Nonnull
    protected abstract State getCurrentState();

    @Override
    public int onGetIndex(@Nonnull final Class<?> clazz) {
        return getCurrentState().getIndex(clazz);
    }

    @Override
    public int onUpdateIndex(@Nonnull final Class<?> clazz, @Nonnull final IntUnaryOperator updateFn) {
        return getCurrentState().updateIndex(clazz, updateFn);
    }

    @Override
    public void onRegisterExpression(@Nonnull final RelationalExpression expression) {
        getCurrentState().registerExpression(expression);
    }

    @Override
    public void onRegisterReference(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        getCurrentState().registerReference(reference);
    }

    @Override
    public void onRegisterQuantifier(@Nonnull final Quantifier quantifier) {
        getCurrentState().registerQuantifier(quantifier);
    }

    @Nullable
    @Override
    public String nameForObject(@Nonnull final Object object) {
        final State state = getCurrentState();
        if (object instanceof RelationalExpression) {
            @Nullable final Integer id = state.getInvertedExpressionsCache().getIfPresent(object);
            return (id == null) ? null : "exp" + id;
        } else if (object instanceof ExpressionRef) {
            @Nullable final Integer id = state.getInvertedReferenceCache().getIfPresent(object);
            return (id == null) ? null : "ref" + id;
        }  else if (object instanceof Quantifier) {
            @Nullable final Integer id = state.getInvertedQuantifierCache().getIfPresent(object);
            return (id == null) ? null : "qun" + id;
        }

        return null;
    }
}
