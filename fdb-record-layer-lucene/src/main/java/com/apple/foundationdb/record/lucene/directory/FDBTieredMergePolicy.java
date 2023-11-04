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

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;

@ParametersAreNonnullByDefault
class FDBTieredMergePolicy extends TieredMergePolicy {
    private final IndexDeferredMaintenancePolicy deferredPolicy;
    private final FDBRecordContext context;

    public FDBTieredMergePolicy(IndexDeferredMaintenancePolicy deferredPolicy, FDBRecordContext context) {
        this.deferredPolicy = deferredPolicy;
        this.context = context;
    }

    boolean isAutoMergeDuringCommit(MergeTrigger mergeTrigger) {
        return mergeTrigger == MergeTrigger.FULL_FLUSH ||
               mergeTrigger == MergeTrigger.COMMIT;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (!deferredPolicy.shouldAutoMergeDuringCommit() && isAutoMergeDuringCommit(mergeTrigger)) {
            // Here: skip it. The merge should be performed later by the user.
            return null;
        }
        long startTime = System.nanoTime();
        final MergeSpecification spec = super.findMerges(mergeTrigger, infos, mergeContext);
        context.record(LuceneEvents.Events.LUCENE_FIND_MERGES, System.nanoTime() - startTime);
        final int diluteLevel = deferredPolicy.getDiluteLevel();
        if (diluteLevel > 0) {
            final int size = spec.merges.size();
            final int newSize = Math.max(1, size >> diluteLevel);
            if (size <= newSize) {
                // Here: cannot dilute (could also happen normally at the tail of a merge session)
                deferredPolicy.setDilutedResults(IndexDeferredMaintenancePolicy.DilutedResults.NOT_DILUTED);
            } else {
                // Here: dilute needed. However, cannot dilute the original spec - must create a new one.
                deferredPolicy.setDilutedResults(IndexDeferredMaintenancePolicy.DilutedResults.HAS_MORE);
                MergeSpecification dilutedSpec = new MergeSpecification();
                for (int i = 0; i < newSize; i++) {
                    dilutedSpec.add(spec.merges.get(i));
                }
                return dilutedSpec;
            }
        }
        return spec;
    }
}

