/*
 * PlanValidator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;

import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("PMD.MissingSerialVersionUID")
public final class PlanValidator {

    private PlanValidator() {
    }

    public static void validate(@Nonnull final MetricCollector metricCollector, @Nonnull final RecordQueryPlan plan,
                                @Nonnull final QueryExecutionParameters context,
                                @Nonnull final PlanHashMode currentPlanHashMode,
                                @Nonnull final List<PlanHashMode> validPlanHashModes) throws RelationalException {
        try {
            // TODO: Parsing the continuation here is a bit of a waste as it also being parsed elsewhere
            ContinuationImpl continuation = ContinuationImpl.parseContinuation(context.getContinuation());
            if (!validateBindingHash(context, continuation)) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                throw new PlanValidationException("Continuation binding does not match query");
            }
            final var resolvedPlanHashMode =
                    resolveValidPlanHashMode(plan, continuation, currentPlanHashMode, validPlanHashModes);
            if (resolvedPlanHashMode == null) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                throw new PlanValidationException("Continuation plan does not match query");
            }
            if (resolvedPlanHashMode != currentPlanHashMode) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }
        } catch (InvalidProtocolBufferException e) {
            metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
            throw new RelationalException("Continuation cannot be parsed", ErrorCode.INVALID_CONTINUATION, e);
        }
        metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED);
    }

    private static boolean validateBindingHash(QueryExecutionParameters parameters, ContinuationImpl continuation) {
        if (continuation.atBeginning()) {
            // No continuation provided - nothing to validate against
            return true;
        } else {
            return Objects.equals(parameters.getParameterHash(), continuation.getBindingHash());
        }
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
                                                         @Nonnull final List<PlanHashMode> validPlanHashModes) {
        if (continuation.atBeginning()) {
            // No continuation provided - nothing to validate against
            return currentPlanHashMode;
        } else {
            // loop through the valid modes assuming that the most likely mode comes first
            for (final var validPlanHashMode : validPlanHashModes) {
                if (Objects.equals(plan.planHash(validPlanHashMode), continuation.getPlanHash())) {
                    return validPlanHashMode;
                }
            }
            return null;
        }
    }

    public static class PlanValidationException extends RelationalException {
        public PlanValidationException(String message) {
            super(message, ErrorCode.INVALID_CONTINUATION);
        }

        public PlanValidationException(String message, Throwable cause) {
            super(message, ErrorCode.INVALID_CONTINUATION, cause);
        }
    }
}
