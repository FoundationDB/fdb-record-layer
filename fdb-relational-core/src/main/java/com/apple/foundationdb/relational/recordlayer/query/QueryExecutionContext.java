/*
 * QueryExecutionContext.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;

public interface QueryExecutionContext {

    @Nonnull
    default EvaluationContext getEvaluationContext(@Nonnull TypeRepository typeRepository) {
        final var builder = EvaluationContext.newBuilder();
        final var constantsMap = new LinkedHashMap<String, Object>();

        final var literals = getLiterals();
        if (!literals.isEmpty()) {
            constantsMap.putAll(literals.asMap());
        }

        final var evaluationConstantsMap = getEvaluationConstants().asMapByConstantId();
        Assert.thatUnchecked(
                Sets.intersection(constantsMap.keySet(), evaluationConstantsMap.keySet()).isEmpty(),
                ErrorCode.INTERNAL_ERROR,
                "conflict between the IDs of evaluation constants and IDs of ordered literals");
        constantsMap.putAll(evaluationConstantsMap);

        builder.setConstant(Quantifier.constant(), Collections.unmodifiableMap(constantsMap));
        return builder.build(typeRepository);
    }

    @Nonnull
    default EvaluationContext getEvaluationContext() {
        return getEvaluationContext(ParseHelpers.EMPTY_TYPE_REPOSITORY);
    }

    @Nonnull
    ExecuteProperties.Builder getExecutionPropertiesBuilder();

    @Nullable
    byte[] getContinuation();

    int getParameterHash();

    @Nonnull
    Literals getLiterals();

    boolean isForExplain(); // todo (yhatem) remove.

    @Nonnull
    QueryEvaluationConstants getEvaluationConstants();

    @Nonnull
    PlanHashable.PlanHashMode getPlanHashMode();
}
