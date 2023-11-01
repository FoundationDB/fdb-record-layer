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
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;

class FDBTieredMergePolicy extends TieredMergePolicy {
    final boolean shouldAutoMergeDuringCommit;
    final FDBRecordContext context;

    public FDBTieredMergePolicy(final boolean shouldAutoMergeDuringCommit, FDBRecordContext context) {
        this.shouldAutoMergeDuringCommit = shouldAutoMergeDuringCommit;
        this.context = context;
    }

    boolean isAutoMergeDuringCommit(MergeTrigger mergeTrigger) {
        return mergeTrigger == MergeTrigger.FULL_FLUSH ||
               mergeTrigger == MergeTrigger.COMMIT;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (!shouldAutoMergeDuringCommit && isAutoMergeDuringCommit(mergeTrigger)) {
            // Here: skip it. The merge should be performed later by the user.
            return null;
        }
        long startTime = System.nanoTime();
        final MergeSpecification spec = super.findMerges(mergeTrigger, infos, mergeContext);
        context.record(LuceneEvents.Events.LUCENE_FIND_MERGES, System.nanoTime() - startTime);
        return spec;
    }
}

