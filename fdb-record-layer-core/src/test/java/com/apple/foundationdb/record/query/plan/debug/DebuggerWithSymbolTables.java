/*
 * PlannerRepl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.RestartException;
import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntUnaryOperator;

/**
 * Implementation of a debugger that maintains symbol tables for easier human consumption e.g. in test cases and/or
 * while debugging.
 */
public class DebuggerWithSymbolTables implements Debugger {
    private static final Logger logger = LoggerFactory.getLogger(DebuggerWithSymbolTables.class);

    private final Deque<State> stateStack;

    @Nullable
    private String queryAsString;
    @Nullable
    private PlanContext planContext;

    public DebuggerWithSymbolTables() {
        this.stateStack = new ArrayDeque<>();
        this.planContext = null;
    }

    @Nonnull
    State getCurrentState() {
        return Objects.requireNonNull(stateStack.peek());
    }

    @Nullable
    public PlanContext getPlanContext() {
        return planContext;
    }

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

    @Override
    public void onInstall() {
        // do nothing
    }

    @Override
    public void onSetup() {
        reset();
    }

    @Override
    public void onShow(@Nonnull final ExpressionRef<? extends RelationalExpression> ref) {
        // do nothing
    }

    @Override
    public void onQuery(@Nonnull final String recordQuery, @Nonnull final PlanContext planContext) {
        this.stateStack.push(State.copyOf(getCurrentState()));
        this.queryAsString = recordQuery;
        this.planContext = planContext;

        logQuery();
    }

    void restartState() {
        stateStack.pop();
        stateStack.push(State.copyOf(getCurrentState()));
    }

    @Override
    public void onEvent(final Event event) {
        Objects.requireNonNull(queryAsString);
        Objects.requireNonNull(planContext);
        getCurrentState().addCurrentEvent(event);
    }

    @Nullable
    private static <T> T lookupInCache(final Cache<Integer, T> cache, final String identifier, final String prefix) {
        @Nullable final Integer refId = getIdFromIdentifier(identifier, prefix);
        if (refId == null) {
            return null;
        }
        return cache.getIfPresent(refId);
    }

    @Nullable
    static Integer getIdFromIdentifier(final String identifier, final String prefix) {
        final String idAsString = identifier.substring(prefix.length());
        try {
            return Integer.valueOf(idAsString);
        } catch (final NumberFormatException numberFormatException) {
            return null;
        }
    }

    @Nonnull
    String nameForObjectOrNotInCache(@Nonnull final Object object) {
        return Optional.ofNullable(nameForObject(object)).orElse("not in cache");
    }

    boolean isValidEntityName(@Nonnull final String identifier) {
        final String lowerCase = identifier.toLowerCase();
        if (!lowerCase.startsWith("exp") &&
                !lowerCase.startsWith("ref") &&
                !lowerCase.startsWith("qun")) {
            return false;
        }

        return getIdFromIdentifier(identifier, identifier.substring(0, 3)) != null;
    }


    @Nullable
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

    @Override
    public void onDone() {
        reset();
    }

    private void reset() {
        this.stateStack.clear();
        this.stateStack.push(State.initial());
        this.planContext = null;
        this.queryAsString = null;
    }

    void logQuery() {
        logger.debug(KeyValueLogMessage.of("planning started", "query", queryAsString));
    }

    @Nonnull
    private <T> Optional<T> getSilently(@Nonnull final String actionName, @Nonnull final SupplierWithException<T> supplier) {
        try {
            return Optional.ofNullable(supplier.get());
        } catch (final RestartException rE) {
            throw rE;
        } catch (final Throwable t) {
            logger.warn("unable to get " + actionName + ": " + t.getMessage());
            t.printStackTrace();
            return Optional.empty();
        }
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {

        /**
         * Gets a result.
         *
         * @return a result
         */
        T get() throws Exception;
    }
}
