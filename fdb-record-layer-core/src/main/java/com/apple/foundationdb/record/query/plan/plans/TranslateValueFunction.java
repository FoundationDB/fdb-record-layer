/*
 * TranslateValueFunction.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Interface for functions employed by {@link RecordQueryFetchFromPartialRecordPlan} that are used to translate a value
 * by conceptually pushing it through that operation.
 */
@FunctionalInterface
public interface TranslateValueFunction {
    TranslateValueFunction UNABLE_TO_TRANSLATE = (v, i) -> Optional.empty();

    /**
     * Translate a value to an equivalent value that can be evaluated prior to the operation that we are trying to push
     * through. That operation normally is a {@link RecordQueryFetchFromPartialRecordPlan} but could potentially be
     * any {@link com.apple.foundationdb.record.query.plan.temp.RelationalExpression}.
     * @param value a value that is correlated to a quantifier ranging over the current expression or that is
     *        only externally correlated. External correlations do not matter for translations as they are still
     *        valid after pushing the value though the current operation.
     * @param baseAlias an alias that the referencing sub-values should be rebased to
     * @return an optional containing the translated value if this method is successful in translating the {@code value}
     *         passed in, {@code Optional.empty()} if the value passed in could not be translated.
     */
    @Nonnull
    Optional<Value> translateValue(@Nonnull Value value,
                                   @Nonnull CorrelationIdentifier baseAlias);

    /**
     * Shorthand for a function that never translates any value successfully.
     * @return a translation function that never translates any given value successfully
     */
    static TranslateValueFunction unableToTranslate() {
        return UNABLE_TO_TRANSLATE;
    }
}
