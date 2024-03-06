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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
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
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
class FDBTieredMergePolicy extends TieredMergePolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBTieredMergePolicy.class);
    @Nullable private final IndexDeferredMaintenanceControl mergeControl;
    private final AgilityContext context;
    @Nonnull
    private final Subspace indexSubspace;
    @Nonnull
    private final Tuple key;

    public FDBTieredMergePolicy(@Nullable IndexDeferredMaintenanceControl mergeControl,
                                @Nonnull AgilityContext context,
                                @Nonnull Subspace indexSubspace,
                                @Nonnull final Tuple key) {
        this.mergeControl = mergeControl;
        this.context = context;
        this.indexSubspace = indexSubspace;
        this.key = key;
    }

    boolean isAutoMergeDuringCommit(MergeTrigger mergeTrigger) {
        return mergeTrigger == MergeTrigger.FULL_FLUSH ||
               mergeTrigger == MergeTrigger.COMMIT;
    }

    private int specSize(@Nullable MergeSpecification spec) {
        return spec != null && spec.merges != null ? spec.merges.size() : 0;
    }

    @Override
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (mergeControl == null) {
            final MergeSpecification merges = super.findMerges(mergeTrigger, infos, mergeContext);
            logFoundMerges(mergeTrigger, merges);
            return merges;
        }
        if (!mergeControl.shouldAutoMergeDuringCommit() && isAutoMergeDuringCommit(mergeTrigger)) {
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
        logFoundMerges(mergeTrigger, spec);
        return spec;
    }

    private void logFoundMerges(@Nonnull final MergeTrigger mergeTrigger,
                                @Nullable final MergeSpecification merges) {
        if (merges != null && LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("Found Merges",
                    LogMessageKeys.INDEX_SUBSPACE, indexSubspace,
                    LogMessageKeys.KEY, key,
                    LuceneLogMessageKeys.MERGE_TRIGGER, mergeTrigger,
                    LogMessageKeys.AGILITY_CONTEXT, context.getClass().getSimpleName(),
                    LuceneLogMessageKeys.MERGE_SOURCE, simpleSpec(merges)));
        }
    }

    private String simpleSpec(final MergeSpecification merges) {
        return merges.merges.stream().map(merge ->
                        merge.segments.stream().map(segment -> segment.info.name)
                                .collect(Collectors.joining(",", "", "")))
                .collect(Collectors.joining(";"));
    }
}

