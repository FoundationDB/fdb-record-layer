/*
 * FDBTieredMergePolicy.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;

@ParametersAreNonnullByDefault
class FDBTieredMergePolicy extends TieredMergePolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBTieredMergePolicy.class);
    @Nullable private final IndexDeferredMaintenanceControl mergeControl;
    private final AgilityContext context;
    @Nonnull
    private final Subspace indexSubspace;
    @Nonnull
    private final Tuple key;
    @Nullable
    private final Exception exceptionAtCreation;

    public FDBTieredMergePolicy(@Nullable IndexDeferredMaintenanceControl mergeControl,
                                @Nonnull AgilityContext context,
                                @Nonnull Subspace indexSubspace,
                                @Nonnull final Tuple key,
                                @Nullable final Exception exceptionAtCreation) {
        this.mergeControl = mergeControl;
        this.context = context;
        this.indexSubspace = indexSubspace;
        this.key = key;
        this.exceptionAtCreation = exceptionAtCreation;
    }

    public static boolean usesCreationStack() {
        return LOGGER.isDebugEnabled();
    }

    private int specSize(@Nullable MergeSpecification spec) {
        return spec != null && spec.merges != null ? spec.merges.size() : 0;
    }

    @Override
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (mergeControl == null) {
            final MergeSpecification merges = super.findMerges(mergeTrigger, infos, mergeContext);
            MergeUtils.logFoundMerges(LOGGER, "Found Merges without mergeControl", context, indexSubspace, key, mergeTrigger, merges, exceptionAtCreation);
            return merges;
        }
        if (!mergeControl.shouldAutoMergeDuringCommit() && !mergeControl.isExplicitMergePath()) {
            // Here: skip it. The merge should be performed later by the user.
            return null;
        }
        long startTime = System.nanoTime();

        MergeSpecification spec = super.findMerges(mergeTrigger, infos, mergeContext);
        final long mergesLimit = mergeControl.getMergesLimit();
        int originSpecSize = specSize(spec);
        mergeControl.setMergesFound(originSpecSize);
        if (mergesLimit > 0 && originSpecSize > 0 && mergesLimit < originSpecSize) {
            // Note: should not dilute merges in the spec object, must create a new one
            MergeSpecification dilutedSpec = new MergeSpecification();
            for (int i = 0; i < mergesLimit; i++) {
                dilutedSpec.add(spec.merges.get(i));
            }
            spec = dilutedSpec;
        }
        mergeControl.setMergesTried(specSize(spec));

        context.recordEvent(LuceneEvents.Events.LUCENE_FIND_MERGES, System.nanoTime() - startTime);
        MergeUtils.logFoundMerges(LOGGER, "Found Merges", context, indexSubspace, key, mergeTrigger, spec, exceptionAtCreation);
        return spec;
    }
}

