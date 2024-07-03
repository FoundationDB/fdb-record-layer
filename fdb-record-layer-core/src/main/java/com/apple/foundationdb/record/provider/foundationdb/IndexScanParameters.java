/*
 * IndexScanParameters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

/**
 * The parameterized form of index scan bounds.
 * These parameters are stored in the plan and then bound to the context before passing on to the index maintainer.
 */
@API(API.Status.UNSTABLE)
public interface IndexScanParameters extends PlanHashable, Correlated<IndexScanParameters>, PlanSerializable {
    /**
     * Get the type of index scan to be performed.
     * @return the scan type
     */
    @Nonnull
    IndexScanType getScanType();

    /**
     * Get the bound form of the index scan for use by the maintainer.
     * An {@code IndexScanParameters} can contain parameter references that must be resolved against a context bindings
     * to get the actual scan bounds. Scan types that use forms other than {@link IndexScanComparisons} can also resolve
     * similar information from the store.
     * @param store store against which the scan will be performed
     * @param index index to be scanned
     * @param context query parameters for the scan
     * @return the index scan bounds
     */
    @Nonnull
    IndexScanBounds bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context);

    /**
     * Get whether this scan is fully restricted, so that it can only output one or zero entries.
     * @param index the index against which the scan will be conducted
     * @return {@code true} if this scan is unique
     */
    boolean isUnique(@Nonnull Index index);

    /**
     * Get a short summary of the scan, not including the scan type.
     * @return the scan details
     */
    @Nonnull
    String getScanDetails();

    /**
     * Get details for graph visualization, including the scan type.
     * @param detailsBuilder builder into which to put details
     * @param attributeMapBuilder builder into which to put attributes
     */
    void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder);

    @Nonnull
    IndexScanParameters translateCorrelations(@Nonnull TranslationMap translationMap);

    @Nonnull
    PIndexScanParameters toIndexScanParametersProto(@Nonnull PlanSerializationContext serializationContext);

    @Nonnull
    static IndexScanParameters fromIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext,
                                                            @Nonnull final PIndexScanParameters indexScanParametersProto) {
        return (IndexScanParameters)PlanSerialization.dispatchFromProtoContainer(serializationContext, indexScanParametersProto);
    }
}
