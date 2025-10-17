/*
 * OptionUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;

@API(API.Status.INTERNAL)
public final class OptionsUtils {

    @Nonnull
    public static PlanHashable.PlanHashMode getCurrentPlanHashMode(@Nonnull final Options options) {
        final String planHashModeAsString = options.getOption(Options.Name.CURRENT_PLAN_HASH_MODE);
        return planHashModeAsString == null ?
               PlanHashable.CURRENT_FOR_CONTINUATION :
               PlanHashable.PlanHashMode.valueOf(planHashModeAsString);
    }

    @Nonnull
    public static Set<PlanHashable.PlanHashMode> getValidPlanHashModes(@Nonnull final Options options) {
        final String planHashModesAsString = options.getOption(Options.Name.VALID_PLAN_HASH_MODES);
        if (planHashModesAsString == null) {
            return ImmutableSet.of(PlanHashable.CURRENT_FOR_CONTINUATION);
        }

        return Arrays.stream(planHashModesAsString.split(","))
                .map(planHashModeAsString -> PlanHashable.PlanHashMode.valueOf(planHashModeAsString.trim()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    public static IndexFetchMethod getIndexFetchMethod(@Nonnull final Options options) {
        Options.IndexFetchMethod indexFetchMethod = options.getOption(Options.Name.INDEX_FETCH_METHOD);
        if (indexFetchMethod == null) {
            return IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK;
        }
        switch (indexFetchMethod) {
            case SCAN_AND_FETCH:
                return IndexFetchMethod.SCAN_AND_FETCH;
            case USE_REMOTE_FETCH:
                return IndexFetchMethod.USE_REMOTE_FETCH;
            case USE_REMOTE_FETCH_WITH_FALLBACK:
                return IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK;
            default:
                throw new RelationalException("Can not convert index fetch method '" + indexFetchMethod.name() + "' to planner configuration",
                        ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
    }
}
