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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenancePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;

@ParametersAreNonnullByDefault
class FDBTieredMergePolicy extends TieredMergePolicy {
    @Nullable private final IndexDeferredMaintenancePolicy deferredPolicy;
    private final FDBRecordContext context;

    public FDBTieredMergePolicy(@Nullable IndexDeferredMaintenancePolicy deferredPolicy, FDBRecordContext context) {
        this.deferredPolicy = deferredPolicy;
        this.context = context;
    }

    boolean isAutoMergeDuringCommit(MergeTrigger mergeTrigger) {
        return mergeTrigger == MergeTrigger.FULL_FLUSH ||
               mergeTrigger == MergeTrigger.COMMIT;
    }

    private int specSize(@Nullable MergeSpecification spec) {
        return spec != null && spec.merges != null ? spec.merges.size() : 0;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (deferredPolicy == null) {
            return super.findMerges(mergeTrigger, infos, mergeContext);
        }
        if (!deferredPolicy.shouldAutoMergeDuringCommit() && isAutoMergeDuringCommit(mergeTrigger)) {
            // Here: skip it. The merge should be performed later by the user.
            return null;
        }
        long startTime = System.nanoTime();
        MergeSpecification spec = super.findMerges(mergeTrigger, infos, mergeContext);
        context.record(LuceneEvents.Events.LUCENE_FIND_MERGES, System.nanoTime() - startTime);
        final long mergesLimit = deferredPolicy.getMergesLimit();
        int originSpecSize = specSize(spec);
        deferredPolicy.setMergesFound(originSpecSize);
        if (mergesLimit > 0 && originSpecSize > 0 && mergesLimit < originSpecSize) {
            // Note: should not dilute merges in the spec object, must create a new one
            MergeSpecification dilutedSpec = new MergeSpecification();
            for (int i = 0; i < mergesLimit; i++) {
                dilutedSpec.add(spec.merges.get(i));
            }
            spec = dilutedSpec;
        }
        deferredPolicy.setMergesTried(specSize(spec));
        return spec;
    }
}

