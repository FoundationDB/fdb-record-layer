/*
 * QueryExecutionContext.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

public interface QueryExecutionContext {

    @Nonnull
    EvaluationContext getEvaluationContext(@Nonnull TypeRepository typeRepository);

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
    PlanHashable.PlanHashMode getPlanHashMode();

}
