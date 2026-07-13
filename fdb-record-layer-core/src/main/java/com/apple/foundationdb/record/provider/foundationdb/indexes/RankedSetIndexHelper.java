/*
 * RankedSetIndexHelper.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Helper functions for index maintainers that use a {@link RankedSet}.
 */
@API(API.Status.INTERNAL)
public class RankedSetIndexHelper {
    public static final Tuple COMPARISON_SKIPPED_SCORE = Tuple.from(Comparisons.COMPARISON_SKIPPED_BINDING);

    /**
     * Parse standard options into {@link RankedSet.Config}.
     * @param index the index definition to get options from
     * @return parsed config options
     */
    public static RankedSet.Config getConfig(@Nonnull Index index) {
        RankedSet.ConfigBuilder builder = RankedSet.newConfigBuilder();
        String hashFunctionOption = index.getOption(IndexOptions.RANK_HASH_FUNCTION);
        if (hashFunctionOption != null) {
            builder.setHashFunction(RankedSetHashFunctions.getHashFunction(hashFunctionOption));
        }
        String nlevelsOption = index.getOption(IndexOptions.RANK_NLEVELS);
        if (nlevelsOption != null) {
            builder.setNLevels(Integer.parseInt(nlevelsOption));
        }
        String duplicatesOption = index.getOption(IndexOptions.RANK_COUNT_DUPLICATES);
        if (duplicatesOption != null) {
            builder.setCountDuplicates(Boolean.parseBoolean(duplicatesOption));
        }
        return builder.build();
    }

    /**
     * Instrumentation events specific to rank index maintenance.
     */
    public enum Events implements StoreTimer.DetailEvent {
        RANKED_SET_SCORE_FOR_RANK("ranked set score for rank"),
        RANKED_SET_RANK_FOR_SCORE("ranked set rank for score"),
        RANKED_SET_UPDATE("ranked set update");

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.DetailEvent.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    private RankedSetIndexHelper() {
    }

    @Nonnull
    public static CompletableFuture<TupleRange> rankRangeToScoreRange(@Nonnull IndexMaintainerState state,
                                                                      int groupPrefixSize,
                                                                      @Nonnull Subspace rankSubspace,
                                                                      @Nonnull RankedSet.Config config,
                                                                      @Nonnull TupleRange rankRange) {
        final Tuple prefix = groupPrefix(groupPrefixSize, rankRange, rankSubspace);
        if (prefix != null) {
            rankSubspace = rankSubspace.subspace(prefix);
        }

        // Map low and high ranks to scores and scan main index with those.
        Number lowRankNum = extractRank(groupPrefixSize, rankRange.getLow());
        boolean startFromBeginning = lowRankNum == null || lowRankNum.longValue() < 0L;
        EndpointType lowEndpoint = startFromBeginning ? EndpointType.RANGE_INCLUSIVE : rankRange.getLowEndpoint();

        Number highRankNum = extractRank(groupPrefixSize, rankRange.getHigh());
        EndpointType highEndpoint = rankRange.getHighEndpoint();
        if (highRankNum != null && (highRankNum.longValue() < 0L || highEndpoint == EndpointType.RANGE_EXCLUSIVE && highRankNum.longValue() == 0L)) {
            // This range is below 0, so we know the range is empty without having to look.
            return CompletableFuture.completedFuture(null);
        }
        if (highRankNum != null && highEndpoint == EndpointType.RANGE_EXCLUSIVE && lowEndpoint == EndpointType.RANGE_EXCLUSIVE && highRankNum.equals(lowRankNum)) {
            // This range is exclusively empty.
            return CompletableFuture.completedFuture(null);
        }

        if (startFromBeginning && highRankNum == null) {
            // Scanning whole range, no need to convert any ranks.
            return CompletableFuture.completedFuture(TupleRange.allOf(prefix));
        }

        final RankedSet rankedSet = new InstrumentedRankedSet(state, rankSubspace, config);
        return init(state, rankedSet).thenCompose(v -> {
            CompletableFuture<Tuple> lowScoreFuture = scoreForRank(state, rankedSet, startFromBeginning ? 0L : lowRankNum, null);
            CompletableFuture<Tuple> highScoreFuture = scoreForRank(state, rankedSet, highRankNum, null);
            return lowScoreFuture.thenCombine(highScoreFuture, (lowScore, highScore) -> {
                // At this point, if either lowScore or highScore are null, it means they are past the end of the list of
                // records. For low, this means the whole range is missed; for high, this means return all of the elements
                // from low until the end.
                if (lowScore == null) {
                    return null;
                }
                EndpointType adjustedHighEndpoint = highScore != null ? highEndpoint :
                        prefix != null ? EndpointType.RANGE_INCLUSIVE : EndpointType.TREE_END;
                TupleRange scoreRange = new TupleRange(lowScore, highScore, lowEndpoint, adjustedHighEndpoint);
                if (prefix != null) {
                    scoreRange = scoreRange.prepend(prefix);
                }
                return scoreRange;
            });
        });
    }

    @Nonnull
    private static CompletableFuture<Void> init(@Nonnull IndexMaintainerState state, @Nonnull RankedSet rankedSet) {
        // The reads that init does can conflict with the atomic mutations that are done to those same keys by add / remove
        // when the key is far enough to the left, as it almost always is for sparser levels.
        // So, check with snapshot read whether it needs to be done first.
        return rankedSet.initNeeded(state.context.readTransaction(true))
                .thenCompose(needed -> needed ? rankedSet.init(state.transaction) : AsyncUtil.DONE);
    }

    @Nullable
    private static Tuple groupPrefix(int groupPrefixSize, @Nonnull TupleRange rankRange, @Nonnull Subspace rankSubspace) {
        if (groupPrefixSize > 0) {
            Tuple lowRank = rankRange.getLow();
            Tuple highRank = rankRange.getHigh();
            if (lowRank == null || lowRank.size() < groupPrefixSize ||
                    highRank == null || highRank.size() < groupPrefixSize) {
                throw new RecordCoreException("Ranked scan range does not include group",
                        "rankRange", rankRange,
                        "rankSubspace", ByteArrayUtil2.loggable(rankSubspace.getKey()));
            }
            Tuple prefix = Tuple.fromList(lowRank.getItems().subList(0, groupPrefixSize));
            Tuple highPrefix = Tuple.fromList(highRank.getItems().subList(0, groupPrefixSize));
            if (!prefix.equals(highPrefix)) {
                throw new RecordCoreException("Ranked scan range crosses groups",
                        "rankRange", rankRange,
                        "rankSubspace", ByteArrayUtil2.loggable(rankSubspace.getKey()));
            }
            return prefix;
        } else {
            return null;
        }
    }

    @Nullable
    private static Number extractRank(int groupPrefixSize, @Nullable Tuple maybeRank) {
        if (maybeRank == null) {
            return null;
        } else if (maybeRank.size() == groupPrefixSize + 1) {
            return (Number)maybeRank.get(groupPrefixSize);
        } else if (maybeRank.size() == groupPrefixSize) {
            return null;
        } else {
            throw new RecordCoreException("Ranked set range bound is not correct size",
                    "groupPrefixSize", groupPrefixSize,
                    "maybeRank", maybeRank);
        }
    }

    public static CompletableFuture<Tuple> scoreForRank(@Nonnull IndexMaintainerState state,
                                                        @Nonnull RankedSet rankedSet,
                                                        @Nullable Number rank,
                                                        @Nullable Tuple outOfRange) {
        if (rank == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            rankedSet.preloadForLookup(state.context.readTransaction(true));
            CompletableFuture<Tuple> result = rankedSet.getNth(state.transaction, rank.longValue())
                    .thenApply(scoreBytes -> scoreBytes == null ? outOfRange : Tuple.fromBytes(scoreBytes));
            if (state.store.getTimer() != null) {
                result = state.store.instrument(Events.RANKED_SET_SCORE_FOR_RANK, result);
            }
            return result;
        }
    }

    public static CompletableFuture<Long> rankForScore(@Nonnull IndexMaintainerState state,
                                                       @Nonnull RankedSet rankedSet,
                                                       @Nullable Tuple score,
                                                       boolean nullIfMissing) {
        if (score == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            rankedSet.preloadForLookup(state.context.readTransaction(true));
            CompletableFuture<Long> result = rankedSet.rank(state.transaction, score.pack(), nullIfMissing);
            return state.store.instrument(Events.RANKED_SET_RANK_FOR_SCORE, result);
        }
    }

    @Nonnull
    public static CompletableFuture<Void> updateRankedSet(@Nonnull IndexMaintainerState state,
                                                          @Nonnull Subspace rankSubspace,
                                                          @Nonnull RankedSet.Config config,
                                                          @Nonnull Tuple valueKey,
                                                          @Nonnull Tuple scoreKey,
                                                          boolean remove) {
        final RankedSet rankedSet = new InstrumentedRankedSet(state, rankSubspace, config);
        final byte[] score = scoreKey.pack();
        CompletableFuture<Void> result = init(state, rankedSet).thenCompose(v -> {
            if (remove) {
                if (config.isCountDuplicates()) {
                    // Decrement count and possibly remove.
                    return removeFromRankedSet(state, rankedSet, score);
                } else {
                    // If no one else has this score, remove from ranked set.
                    return state.transaction.getRange(state.indexSubspace.range(valueKey)).iterator().onHasNext()
                            .thenCompose(hasNext -> hasNext ? AsyncUtil.DONE : removeFromRankedSet(state, rankedSet, score));
                }
            } else {
                return rankedSet.add(state.transaction, score).thenApply(added -> null);
            }
        });
        return state.store.instrument(Events.RANKED_SET_UPDATE, result);
    }

    private static CompletableFuture<Void> removeFromRankedSet(@Nonnull IndexMaintainerState state, @Nonnull RankedSet rankedSet, @Nonnull byte[] score) {
        return rankedSet.remove(state.transaction, score).thenApply(exists -> {
            // It is okay if the score isn't in the ranked set yet if the index is
            // write only because this means that the score just hasn't yet
            // been added by some record. We still want the conflict ranges, though.
            if (!exists && !state.store.isIndexWriteOnly(state.index)) {
                throw new RecordCoreException("Score was not present in ranked set.",
                        "rankSubspace", ByteArrayUtil2.loggable(rankedSet.getSubspace().getKey()));
            }
            return null;
        });
    }

    /**
     * A {@link RankedSet} that adds {@link StoreTimer} instrumentation.
     */
    public static class InstrumentedRankedSet extends RankedSet {
        private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentedRankedSet.class);
        private final FDBTransactionContext context;

        public InstrumentedRankedSet(@Nonnull IndexMaintainerState state,
                                     @Nonnull Subspace rankSubspace,
                                     @Nonnull Config config) {
            super(rankSubspace, state.context.getExecutor(), config);
            this.context = state.context;
        }

        @Override
        public CompletableFuture<Void> init(TransactionContext tc) {
            CompletableFuture<Void> result = super.init(tc);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_INIT, result);
        }

        @Override
        public CompletableFuture<Boolean> contains(ReadTransactionContext tc, byte[] key) {
            CompletableFuture<Boolean> result = super.contains(tc, key);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_CONTAINS, result);
        }

        @Override
        protected CompletableFuture<Boolean> nextLookup(RankedSet.Lookup lookup, ReadTransaction tr) {
            CompletableFuture<Boolean> result = super.nextLookup(lookup, tr);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_NEXT_LOOKUP, result);
        }

        @Override
        protected void nextLookupKey(long duration, boolean newIter, boolean hasNext, int level, boolean rankLookup) {
            if (context.getTimer() != null) {
                FDBStoreTimer.DetailEvents event = FDBStoreTimer.DetailEvents.RANKED_SET_NEXT_LOOKUP_KEY;
                context.getTimer().record(event, duration);
            }
        }

        @Override
        protected int getKeyHash(final byte[] key) {
            final int hash = super.getKeyHash(key);
            if (LOGGER.isTraceEnabled()) {
                final String hashString = Integer.toHexString(hash);
                LOGGER.trace(KeyValueLogMessage.of("Ranked set key hash",
                        LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()),
                        LogMessageKeys.KEY, ByteArrayUtil2.loggable(key),
                        LogMessageKeys.HASH_FUNCTION, RankedSetHashFunctions.getHashFunctionName(config.getHashFunction()),
                        LogMessageKeys.HASH, ("0".repeat(8 - hashString.length()) + hashString)));
            }
            return hash;
        }

        @Override
        protected CompletableFuture<Void> addLevelZeroKey(Transaction tr, byte[] key, int level, boolean increment) {
            CompletableFuture<Void> result = super.addLevelZeroKey(tr, key, level, increment);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_ADD_LEVEL_ZERO_KEY, result);
        }

        @Override
        protected CompletableFuture<Void> addIncrementLevelKey(Transaction tr, byte[] key, int level, boolean orEqual) {
            CompletableFuture<Void> result = super.addIncrementLevelKey(tr, key, level, orEqual);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_ADD_INCREMENT_LEVEL_KEY, result);
        }

        @Override
        protected CompletableFuture<Void> addInsertLevelKey(Transaction tr, byte[] key, int level) {
            CompletableFuture<Void> result = super.addInsertLevelKey(tr, key, level);
            return context.instrument(FDBStoreTimer.DetailEvents.RANKED_SET_ADD_INSERT_LEVEL_KEY, result);
        }
    }

}
