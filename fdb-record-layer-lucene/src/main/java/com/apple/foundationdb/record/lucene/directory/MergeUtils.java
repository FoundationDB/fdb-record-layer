/*
 * MergeUtils.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Collectors;

/**
 * Utility class for helping {@link FDBTieredMergePolicy} and {@code FDBDirectoryMergeScheduler}.
 */
class MergeUtils {


    public static void logExecutingMerge(@Nonnull final Logger logger,
                                         @Nonnull final String staticMessage,
                                         @Nonnull final AgilityContext agilityContext,
                                         @Nonnull final Subspace indexSubspace,
                                         @Nullable final Tuple key,
                                         @Nonnull final MergeTrigger mergeTrigger) {
        if (logger.isDebugEnabled()) {
            final KeyValueLogMessage message = baseLogMessage(staticMessage, agilityContext, indexSubspace, key, mergeTrigger);
            logWithExceptionIfNotAgile(logger, agilityContext, message);
        }
    }

    static void logFoundMerges(@Nonnull final Logger logger,
                               @Nonnull final String staticMessage,
                               @Nonnull final AgilityContext context,
                               @Nonnull final Subspace indexSubspace,
                               @Nullable final Tuple key,
                               @Nonnull final MergeTrigger mergeTrigger,
                               @Nullable final MergePolicy.MergeSpecification merges,
                               @Nullable final Exception exceptionAtCreation) {
        if (merges != null && logger.isDebugEnabled()) {
            final KeyValueLogMessage message = baseLogMessage(staticMessage, context, indexSubspace, key, mergeTrigger);
            message.addKeyAndValue(LuceneLogMessageKeys.MERGE_SOURCE, simpleSpec(merges));
            logWithExceptionIfNotAgile(logger, context, message);
            logWithCreationMessageIfNotAgile(logger, staticMessage, context, indexSubspace, key, mergeTrigger, merges, exceptionAtCreation);
        }
    }

    @SuppressWarnings("PMD.GuardLogStatement") // method is only called in a guard for isDebugEnabled
    private static void logWithCreationMessageIfNotAgile(final @Nonnull Logger logger,
                                                         final @Nonnull String staticMessage,
                                                         final @Nonnull AgilityContext context,
                                                         final @Nonnull Subspace indexSubspace,
                                                         final @Nullable Tuple key,
                                                         final @Nonnull MergeTrigger mergeTrigger,
                                                         final @Nonnull MergePolicy.MergeSpecification merges,
                                                         final @Nullable Exception exceptionAtCreation) {
        if (!(context instanceof AgileContext)) {
            final KeyValueLogMessage message = baseLogMessage(staticMessage, context, indexSubspace, key, mergeTrigger);
            message.addKeyAndValue(LuceneLogMessageKeys.MERGE_SOURCE, simpleSpec(merges));
            logger.debug(message + " (Creation)", exceptionAtCreation);
        }
    }

    @SuppressWarnings("PMD.GuardLogStatement") // method is only called in a guard for isDebugEnabled
    private static void logWithExceptionIfNotAgile(final @Nonnull Logger logger, final @Nonnull AgilityContext context, final KeyValueLogMessage message) {
        if (context instanceof AgileContext) {
            logger.debug(message.toString());
        } else {
            logger.debug(message.toString(), new Exception());
        }
    }

    @Nonnull
    private static KeyValueLogMessage baseLogMessage(final @Nonnull String staticMessage, final @Nonnull AgilityContext context, final @Nonnull Subspace indexSubspace, final @Nullable Tuple key, final @Nonnull MergeTrigger mergeTrigger) {
        return KeyValueLogMessage.build(staticMessage,
                LogMessageKeys.INDEX_SUBSPACE, indexSubspace,
                LogMessageKeys.KEY, key,
                LuceneLogMessageKeys.MERGE_TRIGGER, mergeTrigger,
                LogMessageKeys.AGILITY_CONTEXT, context.getClass().getSimpleName());
    }

    private static String simpleSpec(@Nonnull final MergePolicy.MergeSpecification merges) {
        return merges.merges.stream().map(merge ->
                        merge.segments.stream().map(segment -> segment.info.name)
                                .collect(Collectors.joining(",", "", "")))
                .collect(Collectors.joining(";"));
    }
}
