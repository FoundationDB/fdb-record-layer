/*
 * PlanValidator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("PMD.MissingSerialVersionUID")
@API(API.Status.EXPERIMENTAL)
public final class PlanValidator {

    private PlanValidator() {
    }

    public static PlanHashMode validateSerializedPlanSerializationMode(@Nonnull final CompiledStatement compiledStatement,
                                                                       @Nonnull final Set<PlanHashable.PlanHashMode> validPlanHashModes) throws RelationalException {
        final PlanHashable.PlanHashMode planSerializationMode =
                PlanHashable.PlanHashMode.valueOf(compiledStatement.getPlanSerializationMode());
        if (!validPlanHashModes.contains(planSerializationMode)) {
            throw new PlanValidationException("Plan provided by continuation cannot be continued due to an incompatible environment");
        }
        return planSerializationMode;
    }

    public static <M extends Message> void validateContinuationConstraint(@Nonnull final FDBRecordStoreBase<M> fdbRecordStore,
                                                                          @Nonnull final QueryPlanConstraint continuationPlanConstraint) throws RelationalException {
        final Boolean isValid =
                continuationPlanConstraint.getPredicate()
                        .eval(fdbRecordStore, EvaluationContext.EMPTY);
        if (isValid == null || !isValid) {
            throw new PlanValidationException("Plan provided by continuation cannot be continued due to an incompatible environment");
        }
    }

    public static void validateHashes(@Nonnull final ContinuationImpl parsedContinuation,
                                      @Nonnull final MetricCollector metricCollector,
                                      @Nonnull final RecordQueryPlan plan,
                                      @Nonnull final QueryExecutionContext context,
                                      @Nonnull final PlanHashMode currentPlanHashMode,
                                      @Nonnull final Set<PlanHashMode> validPlanHashModes) throws RelationalException {
        // Nothing needs to be validated if this continuation is at the beginning
        if (!parsedContinuation.atBeginning()) {
            if (!validateBindingHash(context, parsedContinuation)) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                throw new PlanValidationException("Continuation binding does not match query");
            }
            final var resolvedPlanHashMode =
                    resolveValidPlanHashMode(plan, parsedContinuation, currentPlanHashMode, validPlanHashModes);
            if (resolvedPlanHashMode == null) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                throw new PlanValidationException("Continuation plan does not match query");
            }
            if (resolvedPlanHashMode != currentPlanHashMode) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }

            metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED);
        }
    }

    private static boolean validateBindingHash(QueryExecutionContext parameters, ContinuationImpl continuation) {
        return Objects.equals(parameters.getParameterHash(), continuation.getBindingHash());
    }

    /**
     * Attempts to resolve the correct {@link PlanHashMode} that corresponds to the {@link RecordQueryPlan}
     * passed in given the continuation presented by the client.
     * @param plan the {@link RecordQueryPlan}
     * @param continuation this continuation
     * @param currentPlanHashMode the current plan hash mode
     * @param validPlanHashModes all valid plan hash modes accepted by the system. This must include
     *        {@code currentPLanHashMode}
     * @return the plan hash mode whose plan hash of the plan matches the plan hash contained in the continuation
     *         match; {@code null} if no such valid plan hash mode could be resolved.
     */
    @Nullable
    private static PlanHashMode resolveValidPlanHashMode(@Nonnull final RecordQueryPlan plan,
                                                         @Nonnull final ContinuationImpl continuation,
                                                         @Nonnull final PlanHashMode currentPlanHashMode,
                                                         @Nonnull final Set<PlanHashMode> validPlanHashModes) {
        if (Objects.equals(plan.planHash(currentPlanHashMode), continuation.getPlanHash())) {
            return currentPlanHashMode;
        }

        for (final var validPlanHashMode : validPlanHashModes) {
            if (validPlanHashMode != currentPlanHashMode &&
                    Objects.equals(plan.planHash(validPlanHashMode), continuation.getPlanHash())) {
                return validPlanHashMode;
            }
        }
        return null;
    }

    @SuppressWarnings("serial")
    public static class PlanValidationException extends RelationalException {
        public PlanValidationException(String message) {
            super(message, ErrorCode.INVALID_CONTINUATION);
        }

        public PlanValidationException(String message, Throwable cause) {
            super(message, ErrorCode.INVALID_CONTINUATION, cause);
        }
    }
}
