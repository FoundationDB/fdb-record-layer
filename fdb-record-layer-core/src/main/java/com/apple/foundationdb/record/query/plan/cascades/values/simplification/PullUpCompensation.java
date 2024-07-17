/*
 * PullUpCompensation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;

/**
 * Functional interface to perform compensation for partially matched {@link Value}-trees.
 */
@FunctionalInterface
public interface PullUpCompensation {
    PullUpCompensation NO_COMPENSATION = (upperBaseAlias, value) -> value;

    @Nonnull
    Value compensate(@Nonnull CorrelationIdentifier upperBaseAlias,
                     @Nonnull Value value);

    @Nonnull
    static PullUpCompensation noCompensation() {
        return NO_COMPENSATION;
    }
}
