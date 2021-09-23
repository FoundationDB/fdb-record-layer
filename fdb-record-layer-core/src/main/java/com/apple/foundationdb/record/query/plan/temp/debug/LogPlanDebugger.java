/*
 * LogPlanDebugger.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

public class LogPlanDebugger extends BasePlanDebugger {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogPlanDebugger.class);

    // A unique identifier for the instance of the debugger. This gets reset every time a new query is planned, and the
    // idea is that using log queries, one can group together all the events for a particular query.
    @Nullable
    private String instanceUuid;

    // The set of operations to log. Null set (faster check for most uses) means "all".
    // Non-null means that only the actions in the set will be logged.
    @Nullable
    private final Set<DebuggerAction> enabledActions;

    // The current state - this keeps all the caches and manipulates them.
    @Nonnull
    private State state;

    public enum DebuggerAction {
        ON_EVENT("onEvent"),
        ON_DONE("onDone"),
        ON_GET_INDEX("onGetIndex"),
        ON_UPDATE_INDEX("onUpdateIndex"),
        ON_REGISTER_EXPRESSION("onRegisterExpression"),
        ON_REGISTER_REFERENCE("onRegisterReference"),
        ON_REGISTER_QUANTIFIER("onRegisterQuantifier"),
        ON_INSTALL("onInstall"),
        ON_SETUP("onSetup"),
        ON_QUERY("onQuery");

        private String visibleName;

        DebuggerAction(final String visibleName) {
            this.visibleName = visibleName;
        }

        public String getVisibleName() {
            return visibleName;
        }
    }

    public LogPlanDebugger() {
        this((HashSet<DebuggerAction>)null);
    }

    public LogPlanDebugger(@Nonnull DebuggerAction... enabledActions) {
        this(new HashSet<>(Arrays.asList(enabledActions)));
    }

    private LogPlanDebugger(@Nullable final Set<DebuggerAction> enabledActions) {
        state = State.initial();
        this.enabledActions = enabledActions;
    }

    @Override
    protected State getCurrentState() {
        return state;
    }

    @Override
    public void onEvent(final Event event) {
        logMessage(DebuggerAction.ON_EVENT, () -> createLogMessage(
                LogMessageKeys.PLANNER_DEBUGGER_NAME, event.getShorthand(),
                LogMessageKeys.PLANNER_DEBUGGER_LOCATION, event.getLocation(),
                LogMessageKeys.DESCRIPTION, event.getDescription()));
    }

    @Override
    public void onDone(final GroupExpressionRef<?> rootExpression) {
        logMessage(DebuggerAction.ON_DONE, () -> createLogMessage());
        instanceUuid = null;
        state = State.initial();
    }

    @Override
    public int onGetIndex(@Nonnull final Class<?> clazz) {
        int index = super.onGetIndex(clazz);
        logMessage(DebuggerAction.ON_GET_INDEX, () -> createLogMessage(LogMessageKeys.PLANNER_DEBUGGER_NAME, clazz.getName() + ':' + index));
        return index;
    }

    @Override
    public int onUpdateIndex(@Nonnull final Class<?> clazz, @Nonnull final IntUnaryOperator updateFn) {
        int index = super.onUpdateIndex(clazz, updateFn);
        logMessage(DebuggerAction.ON_UPDATE_INDEX, () -> createLogMessage(LogMessageKeys.DESCRIPTION, clazz.getName() + ':' + index));
        return index;
    }

    @Override
    public void onRegisterExpression(@Nonnull final RelationalExpression expression) {
        logMessage(DebuggerAction.ON_REGISTER_EXPRESSION, () -> createLogMessage(LogMessageKeys.DESCRIPTION, expression.toString()));
        super.onRegisterExpression(expression);
    }

    @Override
    public void onRegisterReference(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        logMessage(DebuggerAction.ON_REGISTER_REFERENCE, () -> createLogMessage(LogMessageKeys.DESCRIPTION, reference.toString()));
        super.onRegisterReference(reference);
    }

    @Override
    public void onRegisterQuantifier(@Nonnull final Quantifier quantifier) {
        logMessage(DebuggerAction.ON_REGISTER_QUANTIFIER, () -> createLogMessage(LogMessageKeys.DESCRIPTION, quantifier.toString()));
        super.onRegisterQuantifier(quantifier);
    }

    @Override
    public void onInstall() {
        logMessage(DebuggerAction.ON_INSTALL, () -> createLogMessage());
    }

    @Override
    public void onSetup() {
        logMessage(DebuggerAction.ON_SETUP, () -> createLogMessage());
    }

    @Override
    public void onQuery(final RecordQuery recordQuery, final PlanContext planContext) {
        instanceUuid = UUID.randomUUID().toString();
        logMessage(DebuggerAction.ON_QUERY, () -> createLogMessage(LogMessageKeys.DESCRIPTION, recordQuery.toString()));
    }

    @SuppressWarnings("PMD.GuardLogStatement")
    private void logMessage(@Nonnull DebuggerAction action, @Nonnull Supplier<KeyValueLogMessage> messageSupplier) {
        try {
            if (shouldLogAction(action)) {
                KeyValueLogMessage message = messageSupplier.get();
                message.addKeyAndValue(LogMessageKeys.PLANNER_DEBUGGER_ACTION, action.getVisibleName());
                if (instanceUuid != null) {
                    message.addKeyAndValue(LogMessageKeys.PLANNER_DEBUGGER_INSTANCE_UUID, instanceUuid);
                }
                LOGGER.info(message.toString());
            }
        } catch (Exception ex) {
            LOGGER.warn(KeyValueLogMessage.of("plan debugger",
                    LogMessageKeys.PLANNER_DEBUGGER_ACTION, action.getVisibleName(),
                    LogMessageKeys.DESCRIPTION, "Failed to create log message: " + ex.getMessage()));
        }
    }

    @Nonnull
    private KeyValueLogMessage createLogMessage(@Nonnull final Object... keysAndValues) {
        return KeyValueLogMessage.build("plan debugger", keysAndValues);
    }

    private boolean shouldLogAction(@Nonnull DebuggerAction action) {
        return ((enabledActions == null) || enabledActions.contains(action));
    }
}
