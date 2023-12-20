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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;

import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("PMD.MissingSerialVersionUID")
public final class PlanValidator {

    private PlanValidator() {
    }

    public static void validate(@Nonnull RecordQueryPlan plan, @Nonnull QueryExecutionParameters context,
                                @Nonnull List<PlanHashable.PlanHashMode> validPlanHashModes) throws RelationalException {
        try {
            // TODO: Parsing the continuation here is a bit of a waste as it also being parsed elsewhere
            ContinuationImpl continuation = ContinuationImpl.parseContinuation(context.getContinuation());
            if (!validateBindingHash(context, continuation)) {
                throw new PlanValidationException("Continuation binding does not match query");
            }
            if (!validatePlanHash(plan, continuation, validPlanHashModes)) {
                throw new PlanValidationException("Continuation plan does not match query");
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RelationalException("Continuation cannot be parsed", ErrorCode.INVALID_CONTINUATION, e);
        }
    }

    private static boolean validateBindingHash(QueryExecutionParameters parameters, ContinuationImpl continuation) {
        if (continuation.atBeginning()) {
            // No continuation provided - nothing to validate against
            return true;
        } else {
            return Objects.equals(parameters.getParameterHash(), continuation.getBindingHash());
        }
    }

    private static boolean validatePlanHash(RecordQueryPlan plan, ContinuationImpl continuation,
                                            @Nonnull List<PlanHashable.PlanHashMode> validPlanHashModes) {
        if (continuation.atBeginning()) {
            // No continuation provided - nothing to validate against
            return true;
        } else {
            // loop through the valid modes assuming that the most likely mode comes first
            for (final var validPlanHashMode : validPlanHashModes) {
                if (Objects.equals(plan.planHash(validPlanHashMode), continuation.getPlanHash())) {
                    return true;
                }
            }
            return false;
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
