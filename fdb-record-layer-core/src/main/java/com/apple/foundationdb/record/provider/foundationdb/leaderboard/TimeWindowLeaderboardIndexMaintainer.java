/*
 * TimeWindowLeaderboardIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.TimeWindowLeaderboardProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.indexes.AtomicMutation;
import com.apple.foundationdb.record.provider.foundationdb.indexes.RankedSetIndexHelper;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Maintainer for the <code>TIME_WINDOW_LEADERBOARD</code> index type.
 * @see com.apple.foundationdb.record.provider.foundationdb.leaderboard for details of how this works.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowLeaderboardIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeWindowLeaderboardIndexMaintainer.class);

    public TimeWindowLeaderboardIndexMaintainer(IndexMaintainerState state) {
        super(state);
    }

    @Nonnull
    protected CompletableFuture<TimeWindowLeaderboard> oldestLeaderboardMatching(int type, long timestamp) {
        return loadDirectory().thenApply(directory -> directory == null ? null :
                directory.oldestLeaderboardMatching(type, timestamp));
    }

    @Nonnull
    protected CompletableFuture<TimeWindowLeaderboardDirectory> loadDirectory() {
        final Subspace extraSubspace = getSecondarySubspace();
        return state.transaction.get(extraSubspace.pack()).thenApply(bytes -> {
            if (bytes == null) {
                return null;
            }
            TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory.Builder builder = TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory.newBuilder();
            try {
                builder.mergeFrom(bytes);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreStorageException("error decoding leaderboard directory", ex);
            }
            return new TimeWindowLeaderboardDirectory(builder.build());
        });
    }

    protected void saveDirectory(TimeWindowLeaderboardDirectory directory) {
        final Subspace extraSubspace = getSecondarySubspace();
        state.transaction.set(extraSubspace.pack(), directory.toProto().toByteArray());
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                            @Nonnull TupleRange rankRange,
                                            @Nullable byte[] continuation,
                                            @Nonnull ScanProperties scanProperties) {
        if (scanType != IndexScanType.BY_VALUE && scanType != IndexScanType.BY_RANK && scanType != IndexScanType.BY_TIME_WINDOW) {
            throw new RecordCoreException("Can only scan leaderboard index by time window, rank or value.");
        }

        // Decode range arguments.
        final int type;
        final long timestamp;
        final TupleRange leaderboardRange;
        if (scanType == IndexScanType.BY_TIME_WINDOW) {
            // Get oldest leaderboard of type containing timestamp.
            final Tuple lowRank = rankRange.getLow();
            final Tuple highRank = rankRange.getHigh();
            type = (int)lowRank.getLong(0);
            timestamp = lowRank.getLong(1);
            leaderboardRange = new TupleRange(
                    Tuple.fromList(lowRank.getItems().subList(2, lowRank.size())),
                    Tuple.fromList(highRank.getItems().subList(2, highRank.size())),
                    rankRange.getLowEndpoint(),
                    rankRange.getHighEndpoint());
        } else {
            // Get the all-time leaderboard for unqualified rank or value.
            type = TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE;
            timestamp = 0;  // Any value would do.
            leaderboardRange = rankRange;
        }

        final int groupPrefixSize = getGroupingCount();
        final CompletableFuture<TimeWindowLeaderboard> leaderboardFuture = oldestLeaderboardMatching(type, timestamp);
        final CompletableFuture<TupleRange> scoreRangeFuture;
        if (scanType == IndexScanType.BY_VALUE) {
            scoreRangeFuture = leaderboardFuture.thenApply(leaderboard -> leaderboard == null ? null : leaderboardRange);
        } else {
            scoreRangeFuture = leaderboardFuture.thenCompose(leaderboard -> {
                if (leaderboard == null) {
                    return CompletableFuture.completedFuture(null);
                }
                final Subspace extraSubspace = getSecondarySubspace();
                final Subspace leaderboardSubspace = extraSubspace.subspace(leaderboard.getSubspaceKey());
                return RankedSetIndexHelper.rankRangeToScoreRange(state, groupPrefixSize,
                        leaderboardSubspace, leaderboard.getNLevels(), leaderboardRange);
            });
        }
        // Add leaderboard's key to the front and take it off of the results.
        return RecordCursor.fromFuture(getExecutor(), scoreRangeFuture)
                .flatMapPipelined(scoreRange -> {
                    if (scoreRange == null) {
                        return RecordCursor.empty(getExecutor());
                    } else if (scanType == IndexScanType.BY_VALUE && leaderboardFuture.join().isHighScoreFirst()) {
                        // Reverse direction and endpoints and negate score values.
                        final TupleRange indexRange = negateScoreRange(scoreRange).prepend(leaderboardFuture.join().getSubspaceKey());
                        return scan(indexRange, continuation, scanProperties.setReverse(!scanProperties.isReverse()));
                    } else {
                        final TupleRange indexRange = scoreRange.prepend(leaderboardFuture.join().getSubspaceKey());
                        return scan(indexRange, continuation, scanProperties);
                    }
                }, 1)
                .map(kv -> getIndexEntry(kv, groupPrefixSize, leaderboardFuture.join().isHighScoreFirst()));
    }

    // Remove leaderboard key and negate score if necessary.
    protected static IndexEntry getIndexEntry(@Nonnull IndexEntry rawEntry, int groupPrefixSize, boolean isHighScoreFirst) {
        Tuple key = rawEntry.getKey().popFront();
        if (isHighScoreFirst) {
            key = negateScoreForHighScoreFirst(key, groupPrefixSize);
        }
        return new IndexEntry(rawEntry.getIndex(), key, rawEntry.getValue());
    }

    /**
     * Negate the score part of the given range, which is after the group prefix and before the timestamp and any
     * other tiebreakers.
     * @param range the range of scores in normal order
     * @return a range with scores negated so that they sort in reverse order
     */
    protected TupleRange negateScoreRange(@Nonnull TupleRange range) {
        final int groupPrefixSize = getGroupingCount();
        Tuple low = range.getLow();
        Tuple high = range.getHigh();
        EndpointType lowEndpoint = range.getLowEndpoint();
        EndpointType highEndpoint = range.getHighEndpoint();
        if (low == null || low.size() < groupPrefixSize) {
            if (lowEndpoint == EndpointType.TREE_START) {
                lowEndpoint = EndpointType.TREE_END;
            }
        } else {
            low = negateScoreForHighScoreFirst(low, groupPrefixSize);
        }
        if (high == null || high.size() < groupPrefixSize) {
            if (lowEndpoint == EndpointType.TREE_END) {
                lowEndpoint = EndpointType.TREE_START;
            }
        } else {
            high = negateScoreForHighScoreFirst(high, groupPrefixSize);
        }
        return new TupleRange(high, low, highEndpoint, lowEndpoint);
    }

    /**
     * Negate the score element so that it sorts in reverse order to support high score first.
     * @param entry original entry
     * @param position position in {@code Tuple} of the score value
     * @return a new entry with the score negated
     */
    protected static Tuple negateScoreForHighScoreFirst(@Nonnull Tuple entry, int position) {
        return TupleHelpers.set(entry, position, - entry.getLong(position));
    }

    @Nonnull
    @Override
    protected List<IndexEntry> commonKeys(@Nonnull List<IndexEntry> oldIndexKeys,
                                          @Nonnull List<IndexEntry> newIndexKeys) {
        if (oldIndexKeys.equals(newIndexKeys)) {
            // If the scores are completely unchanged, we are okay to skip the update.
            return oldIndexKeys;
        } else {
            // Otherwise, must redo even the common ones.
            // When removing a score, an old one may now be the best.
            // When adding a better score, a previous best needs to be removed from indexes.
            return Collections.emptyList();
        }
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final Subspace extraSubspace = getSecondarySubspace();
        // The value for the index key cannot vary from entry-to-entry, so get the value only from the first entry.
        final Tuple entryValue = indexEntries.isEmpty()
                ? TupleHelpers.EMPTY
                : indexEntries.get(0).getValue();

        return loadDirectory().thenCompose(directory -> {
            if (directory == null) {
                return AsyncUtil.DONE;
            }
            final Map<Tuple, Collection<OrderedScoreIndexKey>> groupedScores =
                    groupOrderedScoreIndexKeys(indexEntries, directory.isHighScoreFirst(), true);
            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Iterable<TimeWindowLeaderboard> directoryEntry : directory.getLeaderboards().values()) {
                for (TimeWindowLeaderboard leaderboard : directoryEntry) {
                    for (Map.Entry<Tuple, Collection<OrderedScoreIndexKey>> groupEntry : groupedScores.entrySet()) {
                        final Optional<OrderedScoreIndexKey> bestContainedScore = groupEntry.getValue().stream()
                                .filter(score -> leaderboard.containsTimestamp(score.timestamp))
                                .findFirst();
                        if (bestContainedScore.isPresent()) {
                            final Tuple groupKey = groupEntry.getKey();
                            final OrderedScoreIndexKey indexKey = bestContainedScore.get();
                            final Tuple leaderboardGroupKey = leaderboard.getSubspaceKey().addAll(groupKey);

                            // Update the ordinary B-tree for this leaderboard.
                            final Tuple entryKey = leaderboardGroupKey.addAll(indexKey.scoreKey);
                            updateOneKey(savedRecord, remove, new IndexEntry(state.index, entryKey, entryValue));

                            // Update the corresponding rankset for this leaderboard.
                            final Subspace rankSubspace = extraSubspace.subspace(leaderboardGroupKey);
                            futures.add(RankedSetIndexHelper.updateRankedSet(state, rankSubspace,
                                    leaderboard.getNLevels(), entryKey, indexKey.scoreKey, remove));
                        }
                    }
                }
            }
            Optional<Long> latestTimestamp = groupedScores.values().stream()
                    .flatMap(Collection::stream).map(OrderedScoreIndexKey::getTimestamp).max(Long::compareTo);
            if (latestTimestamp.isPresent()) {
                // Keep track of the latest timestamp for any indexed entry.
                // Then, if time window update adds an index that starts before then, we have to index existing records.
                state.transaction.mutate(MutationType.MAX, state.indexSubspace.getKey(),
                        AtomicMutation.Standard.encodeSignedLong(latestTimestamp.get()));
            }
            return AsyncUtil.whenAll(futures);
        });
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        return (function.getName().equals(FunctionNames.RANK) ||
                function.getName().equals(FunctionNames.TIME_WINDOW_RANK) ||
                function.getName().equals(FunctionNames.TIME_WINDOW_RANK_AND_ENTRY)) &&
               state.index.getRootExpression().equals(function.getOperand());
    }

    @Override
    @Nonnull
    @SuppressWarnings({"unchecked", "PMD.UnnecessaryLocalBeforeReturn"})
    @SpotBugsSuppressWarnings("BC_UNCONFIRMED_CAST")
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        if (function.getName().equals(FunctionNames.RANK)) {
            final CompletableFuture<Long> rank = timeWindowRankAndEntry(record, TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, 0)
                    .thenApply(re -> re == null ? null : re.getLeft());
            return (CompletableFuture<T>)rank;
        } else if (function.getName().equals(FunctionNames.TIME_WINDOW_RANK)) {
            final TimeWindowRecordFunction<Long> timeWindowRank = (TimeWindowRecordFunction<Long>) function;
            final TimeWindowForFunction timeWindow = timeWindowRank.getTimeWindow();
            final CompletableFuture<Long> rank = timeWindowRankAndEntry(context, timeWindow, record)
                    .thenApply(re -> re == null ? null : re.getLeft());
            return (CompletableFuture<T>)rank;
        } else if (function.getName().equals(FunctionNames.TIME_WINDOW_RANK_AND_ENTRY)) {
            final TimeWindowRecordFunction<Tuple> timeWindowRankAndEntry = (TimeWindowRecordFunction<Tuple>) function;
            final TimeWindowForFunction timeWindow = timeWindowRankAndEntry.getTimeWindow();
            final CompletableFuture<Tuple> rankAndEntry = timeWindowRankAndEntry(context, timeWindow, record)
                    .thenApply(re -> re == null ? null : Tuple.from(re.getLeft()).addAll(re.getRight()));
            return (CompletableFuture<T>)rankAndEntry;
        } else {
            return unsupportedRecordFunction(function);
        }
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        if (FunctionNames.TIME_WINDOW_COUNT.equals(function.getName()) &&
                function.getOperand().equals(state.index.getRootExpression())) {
            return true;
        }
        if ((FunctionNames.SCORE_FOR_TIME_WINDOW_RANK.equals(function.getName()) ||
                FunctionNames.SCORE_FOR_TIME_WINDOW_RANK_ELSE_SKIP.equals(function.getName()) ||
                FunctionNames.TIME_WINDOW_RANK_FOR_SCORE.equals(function.getName())) &&
                function.getOperand().equals(state.index.getRootExpression())) {
            return true;
        }
        return super.canEvaluateAggregateFunction(function);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        if (FunctionNames.TIME_WINDOW_COUNT.equals(function.getName()) && range.isEquals()) {
            return evaluateEqualRange(range, (leaderboard, rankedSet, values) ->
                rankedSet.size(state.context.readTransaction(isolationLevel.isSnapshot())).thenApply(Tuple::from));
        }
        if ((FunctionNames.SCORE_FOR_TIME_WINDOW_RANK.equals(function.getName()) ||
                FunctionNames.SCORE_FOR_TIME_WINDOW_RANK_ELSE_SKIP.equals(function.getName())) &&
                range.isEquals()) {
            final Tuple outOfRange = FunctionNames.SCORE_FOR_TIME_WINDOW_RANK_ELSE_SKIP.equals(function.getName()) ?
                                     RankedSetIndexHelper.COMPARISON_SKIPPED_SCORE : null;
            return evaluateEqualRange(range, (leaderboard, rankedSet, values) ->
                RankedSetIndexHelper.scoreForRank(state, rankedSet, (Number)values.get(0), outOfRange)
                        .thenApply(score -> score == null || !leaderboard.isHighScoreFirst() ? score : negateScoreForHighScoreFirst(score, 0)));
        }
        if (FunctionNames.TIME_WINDOW_RANK_FOR_SCORE.equals(function.getName()) && range.isEquals()) {
            return evaluateEqualRange(range, (leaderboard, rankedSet, values) -> {
                final Tuple scoreValues = leaderboard.isHighScoreFirst() ? negateScoreForHighScoreFirst(values, 0) : values;
                return RankedSetIndexHelper.rankForScore(state, rankedSet, scoreValues, false).thenApply(Tuple::from);
            });
        }
        return unsupportedAggregateFunction(function);
    }

    private interface EvaluateEqualRange {
        @Nonnull
        CompletableFuture<Tuple> apply(@Nonnull TimeWindowLeaderboard leaderboard, @Nonnull RankedSet rankedSet, @Nonnull Tuple values);
    }

    private CompletableFuture<Tuple> evaluateEqualRange(@Nonnull TupleRange range,
                                                        @Nonnull EvaluateEqualRange function) {
        final Tuple tuple = range.getLow();
        final int type = (int) tuple.getLong(0);
        final long timestamp = tuple.getLong(1);
        final int groupingCount = getGroupingCount();
        final Tuple groupKey = TupleHelpers.subTuple(tuple, 2, 2 + groupingCount);
        final Tuple values = TupleHelpers.subTuple(tuple, 2 + groupingCount, tuple.size());
        final CompletableFuture<TimeWindowLeaderboard> leaderboardFuture = oldestLeaderboardMatching(type, timestamp);
        return leaderboardFuture.thenCompose(leaderboard -> {
            if (leaderboard == null) {
                return CompletableFuture.completedFuture(null);
            }
            final Tuple leaderboardGroupKey = leaderboard.getSubspaceKey().addAll(groupKey);
            final Subspace extraSubspace = getSecondarySubspace();
            final Subspace rankSubspace = extraSubspace.subspace(leaderboardGroupKey);
            final RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, leaderboard.getNLevels());
            return function.apply(leaderboard, rankedSet, values);
        });
    }

    @Nonnull
    public <M extends Message> CompletableFuture<Pair<Long,Tuple>> timeWindowRankAndEntry(@Nonnull EvaluationContext context,
                                                                                          @Nonnull TimeWindowForFunction timeWindow,
                                                                                          @Nonnull FDBRecord<M> record) {
        return timeWindowRankAndEntry(record, timeWindow.getLeaderboardType(context), timeWindow.getLeaderboardTimestamp(context));
    }

    @Nonnull
    public <M extends Message> CompletableFuture<Pair<Long,Tuple>> timeWindowRankAndEntry(@Nonnull FDBRecord<M> record,
                                                                                          int type, long timestamp) {
        final List<IndexEntry> indexEntries = evaluateIndex(record);

        final CompletableFuture<TimeWindowLeaderboard> leaderboardFuture = oldestLeaderboardMatching(type, timestamp);
        return leaderboardFuture.thenCompose(leaderboard -> {
            if (leaderboard == null) {
                return CompletableFuture.completedFuture(null);
            }

            final Map<Tuple, Collection<OrderedScoreIndexKey>> groupedScores =
                    groupOrderedScoreIndexKeys(indexEntries, leaderboard.isHighScoreFirst(), true);
            if (groupedScores.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (groupedScores.size() > 1) {
                throw new RecordCoreException("Record has more than one group of scores");
            }

            Map.Entry<Tuple, Collection<OrderedScoreIndexKey>> groupEntry = groupedScores.entrySet().iterator().next();
            Optional<OrderedScoreIndexKey> bestContainedScore = groupEntry.getValue().stream()
                    .filter(score -> leaderboard.containsTimestamp(score.timestamp))
                    .findFirst();
            if (!bestContainedScore.isPresent()) {
                return CompletableFuture.completedFuture(null);
            }

            // bestContainedScore should be the one stored in the leaderboard's ranked set; get its rank there.
            final Tuple groupKey = groupEntry.getKey();
            final OrderedScoreIndexKey indexKey = bestContainedScore.get();
            final Tuple leaderboardGroupKey = leaderboard.getSubspaceKey().addAll(groupKey);
            final Subspace extraSubspace = getSecondarySubspace();
            final Subspace rankSubspace = extraSubspace.subspace(leaderboardGroupKey);
            final RankedSet rankedSet = new RankedSetIndexHelper.InstrumentedRankedSet(state, rankSubspace, leaderboard.getNLevels());
            // Undo any negation needed to find entry.
            final Tuple entry = leaderboard.isHighScoreFirst() ? negateScoreForHighScoreFirst(indexKey.scoreKey, 0) : indexKey.scoreKey;
            return RankedSetIndexHelper.rankForScore(state, rankedSet, indexKey.scoreKey, true).thenApply(rank -> Pair.of(rank, entry));
        });
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return loadDirectory().thenApply(directory -> {
            if (directory != null) {
                final Subspace indexSubspace = getIndexSubspace();
                final Subspace extraSubspace = getSecondarySubspace();
                for (Iterable<TimeWindowLeaderboard> directoryEntry : directory.getLeaderboards().values()) {
                    for (TimeWindowLeaderboard leaderboard : directoryEntry) {
                        // Range deletes are used for the more common operation of deleting a time window, so we need
                        // to do each one here to get to its grouping.
                        final Tuple leaderboardGroupKey = leaderboard.getSubspaceKey().addAll(prefix);
                        // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we
                        // may store something at the group root as well.
                        final byte[] indexKey = indexSubspace.pack(leaderboardGroupKey);
                        tr.clear(indexKey, ByteArrayUtil.strinc(indexKey));
                        final byte[] ranksetKey = extraSubspace.pack(leaderboardGroupKey);
                        tr.clear(ranksetKey, ByteArrayUtil.strinc(ranksetKey));
                    }
                }
            }
            return null;
        });
    }

    @Override
    @Nonnull
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        CompletableFuture<IndexOperationResult> result;
        StoreTimer.Event event = null;
        if (operation instanceof TimeWindowLeaderboardWindowUpdate) {
            final UpdateState state = new UpdateState((TimeWindowLeaderboardWindowUpdate)operation);
            result = state.loadDirectory()
                    .thenApply(directory -> {
                        state.setDirectory(directory);
                        return null;
                    })
                    .thenApply(vignore -> {
                        state.update();
                        return null;
                    })
                    .thenCompose(vignore -> state.checkRebuild())
                    .thenCompose(vignore -> state.save())
                    .thenApply(vignore -> state.getResult());
            event = FDBStoreTimer.Events.TIME_WINDOW_LEADERBOARD_GET_DIRECTORY;
        } else if (operation instanceof TimeWindowLeaderboardScoreTrim) {
            final TimeWindowLeaderboardScoreTrim trim = (TimeWindowLeaderboardScoreTrim)operation;
            result = loadDirectory().thenApply(directory -> new TimeWindowLeaderboardScoreTrimResult(
                    trimScores(directory, trim.getScores(), trim.isIncludesGroup())));
            event = FDBStoreTimer.Events.TIME_WINDOW_LEADERBOARD_UPDATE_DIRECTORY;
        } else if (operation instanceof TimeWindowLeaderboardDirectoryOperation) {
            result = loadDirectory().thenApply(TimeWindowLeaderboardDirectoryResult::new);
            event = FDBStoreTimer.Events.TIME_WINDOW_LEADERBOARD_TRIM_SCORES;
        } else {
            result = super.performOperation(operation);
        }
        if (event != null && getTimer() != null) {
            result = getTimer().instrument(event, result, getExecutor());
        }
        return result;
    }

    protected class UpdateState {
        private final TimeWindowLeaderboardWindowUpdate update;
        private TimeWindowLeaderboardDirectory directory;
        private boolean rebuild;
        private boolean changed;
        private long earliestAddedStartTimestamp;

        public UpdateState(TimeWindowLeaderboardWindowUpdate update) {
            this.update = update;
            rebuild = update.getRebuild() == TimeWindowLeaderboardWindowUpdate.Rebuild.ALWAYS;
        }

        protected boolean isRebuildConditional() {
            return !rebuild &&
                update.getRebuild() == TimeWindowLeaderboardWindowUpdate.Rebuild.IF_OVERLAPPING_CHANGED;
        }

        public CompletableFuture<TimeWindowLeaderboardDirectory> loadDirectory() {
            if (rebuild) {
                return CompletableFuture.completedFuture(null);
            } else {
                return TimeWindowLeaderboardIndexMaintainer.this.loadDirectory();
            }
        }

        public void setDirectory(@Nullable TimeWindowLeaderboardDirectory existingDirectory) {
            directory = existingDirectory;

            if (directory != null && directory.isHighScoreFirst() != update.isHighScoreFirst()) {
                if (update.getRebuild() == TimeWindowLeaderboardWindowUpdate.Rebuild.NEVER) {
                    throw new RecordCoreException("cannot change highScoreFirst without a rebuild");
                }
                directory = null;
            }

            if (directory == null) {
                directory = new TimeWindowLeaderboardDirectory(update.isHighScoreFirst());
                rebuild = true;
            }
        }

        public void update() {
            directory.setUpdateTimestamp(update.getUpdateTimestamp());

            final Subspace indexSubspace = getIndexSubspace();
            final Subspace extraSubspace = getSecondarySubspace();

            for (Iterable<TimeWindowLeaderboard> leaderboards : directory.getLeaderboards().values()) {
                Iterator<TimeWindowLeaderboard> iter = leaderboards.iterator();
                while (iter.hasNext()) {
                    TimeWindowLeaderboard leaderboard = iter.next();
                    if (update.getDeleteBefore() >= leaderboard.getEndTimestamp()) {
                        state.transaction.clear(indexSubspace.pack(leaderboard.getSubspaceKey()));
                        state.transaction.clear(extraSubspace.pack(leaderboard.getSubspaceKey()));
                        iter.remove();
                        changed = true;
                        if (getTimer() != null) {
                            getTimer().increment(FDBStoreTimer.Counts.TIME_WINDOW_LEADERBOARD_DELETE_WINDOW);
                        }
                    }
                }
            }
            if (update.isAllTime()) {
                Collection<TimeWindowLeaderboard> existing = directory.getLeaderboards().get(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE);
                if (existing == null || existing.isEmpty()) {
                    directory.addLeaderboard(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, Long.MIN_VALUE, Long.MAX_VALUE, update.getNlevels());
                    if (isRebuildConditional()) {
                        rebuild = true;
                    }
                    changed = true;
                    if (getTimer() != null) {
                        getTimer().increment(FDBStoreTimer.Counts.TIME_WINDOW_LEADERBOARD_ADD_WINDOW);
                    }
                }
            }
            earliestAddedStartTimestamp = Long.MAX_VALUE;
            for (TimeWindowLeaderboardWindowUpdate.TimeWindowSpec spec : update.getSpecs()) {
                for (int i = 0; i < spec.getCount(); i++) {
                    long startTimestamp = spec.getBaseTimestamp() + spec.getStartIncrement() * i;
                    long endTimestamp = startTimestamp + spec.getDuration();
                    if (directory.findLeaderboard(spec.getType(), startTimestamp, endTimestamp) == null) {
                        directory.addLeaderboard(spec.getType(), startTimestamp, endTimestamp, update.getNlevels());
                        if (earliestAddedStartTimestamp > startTimestamp) {
                            earliestAddedStartTimestamp = startTimestamp;
                        }
                        changed = true;
                        if (getTimer() != null) {
                            getTimer().increment(FDBStoreTimer.Counts.TIME_WINDOW_LEADERBOARD_ADD_WINDOW);
                        }
                    }
                }
            }
        }

        public CompletableFuture<Void> checkRebuild() {
            if (changed && isRebuildConditional()) {
                return state.transaction.get(state.indexSubspace.getKey()).thenApply(maxBytes -> {
                    if (maxBytes != null) {
                        final long latestEntryTimestamp = AtomicMutation.Standard.decodeSignedLong(maxBytes);
                        // If some record has been added since last rebuild that is after the start of a newly
                        // added time window, we have to index existing records, which we currently do by rebuilding.
                        if (latestEntryTimestamp >= earliestAddedStartTimestamp) {
                            rebuild = true;
                            LOGGER.info(KeyValueLogMessage.of("rebuilding leaderboard index due to overlapping existing record",
                                            LogMessageKeys.LATEST_ENTRY_TIMESTAMP, latestEntryTimestamp,
                                            LogMessageKeys.EARLIEST_ADDED_START_TIMESTAMP, earliestAddedStartTimestamp,
                                            LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(state.indexSubspace.pack())));
                            if (getTimer() != null) {
                                getTimer().increment(FDBStoreTimer.Counts.TIME_WINDOW_LEADERBOARD_OVERLAPPING_CHANGED);
                            }
                        }
                    }
                    return null;
                });
            } else {
                return AsyncUtil.DONE;
            }
        }

        public CompletableFuture<Void> save() {
            if (rebuild) {
                deleteWhere(state.transaction, TupleHelpers.EMPTY);
            }
            if (changed) {
                saveDirectory(directory);
            }
            if (rebuild) {
                return state.store.rebuildIndex(state.index);
            } else {
                return AsyncUtil.DONE;
            }
        }

        public TimeWindowLeaderboardWindowUpdateResult getResult() {
            return new TimeWindowLeaderboardWindowUpdateResult(changed, rebuild);
        }
    }

    protected Collection<Tuple> trimScores(@Nullable TimeWindowLeaderboardDirectory directory,
                                                @Nonnull Collection<Tuple> scores, boolean includesGroup) {
        if (directory == null) {
            return scores;
        }
        final List<IndexEntry> indexEntries = scores.stream().map(score -> new IndexEntry(state.index, score, TupleHelpers.EMPTY)).collect(Collectors.toList());
        final Map<Tuple, Collection<OrderedScoreIndexKey>> groupedScores =
                groupOrderedScoreIndexKeys(indexEntries, directory.isHighScoreFirst(), includesGroup);
        final Set<OrderedScoreIndexKey> trimmed = new TreeSet<>();
        for (Iterable<TimeWindowLeaderboard> directoryEntry : directory.getLeaderboards().values()) {
            for (TimeWindowLeaderboard leaderboard : directoryEntry) {
                for (Collection<OrderedScoreIndexKey> entry : groupedScores.values()) {
                    Optional<OrderedScoreIndexKey> bestContainedScore = entry.stream()
                            .filter(score -> leaderboard.containsTimestamp(score.timestamp))
                            .findFirst();
                    bestContainedScore.ifPresent(trimmed::add);
                }
            }
        }
        return trimmed.stream().map(indexKey -> indexKey.getIndexEntry().getKey()).collect(Collectors.toList());
    }

    /**
     * Group the given <code>indexKeys</code> by group of size <code>groupPrefixSize</code>, ordering within each
     * group by score, taking <code>highScoreFirst</code> into account.
     * @param indexEntries index entries to be added to the index
     * @param highScoreFirst whether higher scores are better (earlier in the list)
     * @param includesGroup whether index entries also include the group key(s)
     * @return index keys grouped by leaderboard
     */
    protected Map<Tuple, Collection<OrderedScoreIndexKey>> groupOrderedScoreIndexKeys(@Nonnull Iterable<IndexEntry> indexEntries,
                                                                                       boolean highScoreFirst,
                                                                                       boolean includesGroup) {
        final int groupPrefixSize = getGroupingCount();
        final Map<Tuple, Collection<OrderedScoreIndexKey>> grouped = new HashMap<>();
        for (IndexEntry indexEntry : indexEntries) {
            // group_keys[groupingPrefixSize], score, timestamp, other tiebreakers...
            Tuple scoreKey = indexEntry.getKey();
            Tuple groupKey = TupleHelpers.EMPTY;

            if (highScoreFirst) {
                scoreKey = negateScoreForHighScoreFirst(scoreKey, includesGroup ? groupPrefixSize : 0);
            }

            if (includesGroup && groupPrefixSize > 0) {
                groupKey = TupleHelpers.subTuple(scoreKey, 0, groupPrefixSize);
                scoreKey = TupleHelpers.subTuple(scoreKey, groupPrefixSize, scoreKey.size());
            }
            final OrderedScoreIndexKey orderedScoreIndexKey = new OrderedScoreIndexKey(indexEntry, scoreKey);
            grouped.compute(groupKey, (gignore, collection) -> {
                if (collection == null) {
                    collection = new TreeSet<>();
                }
                collection.add(orderedScoreIndexKey);
                return collection;
            });
        }
        return grouped;
    }

    /**
     * A (potential) index key for a score.
     * Orders by best score first, then earliest timestamp, then other tiebreakers.
     * Does not include group prefix in order comparison, as assumed to be bucketed by that.
     * {@link #groupOrderedScoreIndexKeys}
     */
    static class OrderedScoreIndexKey implements Comparable<OrderedScoreIndexKey> {
        @Nonnull
        final IndexEntry indexEntry;
        @Nonnull
        final Tuple scoreKey;
        final long timestamp;

        public OrderedScoreIndexKey(IndexEntry indexEntry, Tuple scoreKey) {
            this.indexEntry = indexEntry;
            this.scoreKey = scoreKey;

            timestamp = scoreKey.getLong(1);
        }

        @Nonnull
        public IndexEntry getIndexEntry() {
            return indexEntry;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OrderedScoreIndexKey that = (OrderedScoreIndexKey) o;

            return indexEntry.equals(that.indexEntry);
        }

        @Override
        public int hashCode() {
            return indexEntry.hashCode();
        }

        @Override
        public int compareTo(OrderedScoreIndexKey that) {
            return this.scoreKey.compareTo(that.scoreKey);
        }
    }

}
